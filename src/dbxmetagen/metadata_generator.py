from abc import ABC, abstractmethod
import json
import mlflow
from pydantic import ValidationError
from typing import Tuple, Dict, List, Any, Union, Optional
from openai.types.chat.chat_completion import ChatCompletion
from pydantic import BaseModel, ConfigDict, field_validator
from src.dbxmetagen.config import MetadataConfig
from src.dbxmetagen.error_handling import exponential_backoff
from src.dbxmetagen.chat_client import ChatClientFactory


class Response(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: str
    columns: List[str]


class PIColumnContent(BaseModel):
    classification: str
    type: str
    confidence: float


class PIResponse(Response):
    model_config = ConfigDict(extra="forbid")
    column_contents: List[PIColumnContent]
    presidio_results: Optional[str] = None


class CommentResponse(Response):
    model_config = ConfigDict(extra="forbid")
    column_contents: Union[str, list[str]]

    @field_validator("column_contents", mode="before")
    @classmethod
    def validate_column_contents(cls, v):
        """Convert string to list if needed, flatten nested lists, parse stringified arrays."""

        def try_parse_stringified_array(s):
            """Try to parse a string as a JSON array. Returns (success, parsed_list_or_original)."""
            if isinstance(s, str):
                stripped = s.strip()
                if stripped.startswith("["):
                    # Handle truncated arrays - LLM sometimes outputs stringified array
                    # that gets cut off before the closing ]
                    if not stripped.endswith("]"):
                        if stripped.endswith('"') or stripped.endswith("'"):
                            stripped = stripped + "]"
                    try:
                        parsed = json.loads(stripped)
                        if isinstance(parsed, list):
                            return True, [
                                str(item) if not isinstance(item, str) else item
                                for item in parsed
                            ]
                    except json.JSONDecodeError:
                        pass
            return False, s

        if isinstance(v, str):
            # Check if it's a stringified JSON array
            success, result = try_parse_stringified_array(v)
            if success:
                return result
            return [v]
        elif isinstance(v, list):
            # Handle nested list case: [[desc1, desc2, ...]] -> [desc1, desc2, ...]
            if len(v) == 1 and isinstance(v[0], list):
                v = v[0]

            # Handle case where list has ONE element that is a stringified multi-element array
            # e.g., ["[\"desc1\", \"desc2\", \"desc3\"]"] -> ["desc1", "desc2", "desc3"]
            if len(v) == 1 and isinstance(v[0], str):
                success, result = try_parse_stringified_array(v[0])
                if success and len(result) > 1:
                    return result

            # Process each element, expanding any stringified arrays
            expanded = []
            for item in v:
                success, result = try_parse_stringified_array(item)
                if success:
                    expanded.extend(result)
                else:
                    expanded.append(str(item) if not isinstance(item, str) else item)
            return expanded
        else:
            raise ValueError(
                "column_contents must be either a string or a list of strings"
            )


class SummaryCommentResponse(Response):
    pass


class MetadataGenerator(ABC):
    def from_context(self, config):
        self.config = config
        self.chat_client = ChatClientFactory.create_client(config)

    @abstractmethod
    def get_responses(
        self, prompt=None, prompt_content=None
    ) -> Tuple[Response, ChatCompletion]:
        """Abstract method to get responses from the chat client.

        Args:
            prompt: The prompt to use for the chat client.
            prompt_content: The prompt content to use for the chat client.
        """


class CommentGenerator(MetadataGenerator):
    """
    Generate comments for a table.

    Args:
        MetadataGenerator: The parent class.
    """

    def get_responses(
        self, prompt, prompt_content
    ) -> Tuple[CommentResponse, ChatCompletion]:

        prompt_size = len(json.dumps(prompt))
        if prompt_size > self.config.max_prompt_length * 5:
            raise ValueError(
                f"The prompt template is too long ({prompt_size} chars). Please reduce the "
                f"number of columns or increase the max_prompt_length."
            )
        comment_response, message_payload = self.get_comment_response(
            self.config,
            content=prompt_content,
            prompt_content=prompt[self.config.mode],
            model=self.config.model,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature,
        )
        return comment_response, message_payload

    def predict_chat_response(self, prompt_content):
        """
        Predict the chat response using the appropriate chat client.
        """
        # mlflow.set_tag("benchmarking_id", self.config.benchmarking_id)
        self.chat_response = self.chat_client.create_structured_completion(
            messages=prompt_content,
            response_model=CommentResponse,
            model=self.config.model,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature,
        )
        return self.chat_response

    def get_comment_response(
        self,
        config: MetadataConfig,
        content: str,
        prompt_content: str,
        model: str,
        max_tokens: int,
        temperature: float,
        retries: int = 0,
        max_retries: int = 5,
    ) -> Tuple[CommentResponse, Dict[str, Any]]:
        try:
            chat_response = self._get_chat_completion(
                config, prompt_content, model, max_tokens, temperature
            )
            response_payload = None
            return chat_response, response_payload
        except (ValidationError, json.JSONDecodeError, AttributeError, ValueError) as e:
            if retries < max_retries:
                print(f"Attempt {retries + 1} failed, retrying due to {e}...")
                return self.get_comment_response(
                    config,
                    content,
                    prompt_content,
                    model,
                    max_tokens,
                    temperature,
                    retries + 1,
                    max_retries,
                )
            else:
                print("Validation error - response")
                raise ValueError(f"Validation error after {max_retries} attempts: {e}")

    def _get_chat_completion(
        self,
        config: MetadataConfig,
        prompt_content: str,
        model: str,
        max_tokens: int,
        temperature: float,
        retries: int = 0,
        max_retries: int = 0,
    ) -> ChatCompletion:
        try:
            return self.predict_chat_response(prompt_content)
        except Exception as e:
            if retries < max_retries:
                print(f"[RETRY] Error: {e}. Retrying in {2 ** retries} seconds...")
                exponential_backoff(retries)
                return self._get_chat_completion(
                    config,
                    prompt_content,
                    model,
                    max_tokens,
                    temperature,
                    retries + 1,
                    max_retries,
                )
            else:
                print(f"Failed after {max_retries} retries.")
                raise e

    def _parse_response(self, response: str) -> Dict[str, Any]:
        try:
            response_dict = json.loads(response)
            if not isinstance(response_dict, dict):
                raise ValueError("Response is not a valid dict")
            return response_dict
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON decode error: {e}")

    def _validate_response(self, content: str, response_dict: Dict[str, Any]) -> None:
        if not self._check_list_and_dict_keys_match(
            content["column_contents"]["columns"], response_dict["columns"]
        ):
            raise ValueError("Column names do not match column contents")

    @staticmethod
    def _check_list_and_dict_keys_match(dict_list, string_list):
        if isinstance(dict_list, list):
            dict_keys = dict_list
        else:
            try:
                dict_keys = dict_list.keys()
            except:
                raise TypeError("dict_list is not a list or a dictionary")
        list_matches_keys = all(item in dict_keys for item in string_list)
        keys_match_list = all(key in string_list for key in dict_keys)
        if not (list_matches_keys and keys_match_list):
            return False
        return True


class PIIdentifier(MetadataGenerator):
    def get_responses(
        self, prompt, prompt_content
    ) -> Tuple[PIResponse, ChatCompletion]:
        prompt_size = len(json.dumps(prompt))
        if prompt_size > self.config.max_prompt_length * 5:
            raise ValueError(
                f"The prompt template is too long ({prompt_size} chars). Please reduce the "
                f"number of columns or increase the max_prompt_length."
            )
        comment_response, message_payload = self.get_pi_response(
            self.config,
            content=prompt_content,
            prompt_content=prompt[self.config.mode],
            model=self.config.model,
            max_tokens=self.config.max_tokens,
            temperature=self.config.temperature,
        )
        return comment_response, message_payload

    def predict_chat_response(self, prompt_content):
        try:
            self.chat_response = self.chat_client.create_structured_completion(
                messages=prompt_content,
                response_model=PIResponse,
                model=self.config.model,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
            )
            return self.chat_response
        except Exception as e:
            print(f"Validation error - response: {e}")
            raise e

    def get_pi_response(
        self,
        config: MetadataConfig,
        content: str,
        prompt_content: str,
        model: str,
        max_tokens: int,
        temperature: float,
        retries: int = 0,
        max_retries: int = 0,
    ) -> Tuple[PIResponse, Dict[str, Any]]:
        try:
            chat_response = self._get_chat_completion(
                config, prompt_content, model, max_tokens, temperature
            )
            response_payload = None
            return chat_response, response_payload
        except (ValidationError, json.JSONDecodeError, AttributeError, ValueError) as e:
            if retries < max_retries:
                return self.get_pi_response(
                    config,
                    content,
                    prompt_content,
                    model,
                    max_tokens,
                    temperature,
                    retries + 1,
                    max_retries,
                )
            else:
                raise ValueError(
                    f"Validation error after %s attempts: %s", max_retries, e
                )

    def _get_chat_completion(
        self,
        config: MetadataConfig,
        prompt_content: str,
        model: str,
        max_tokens: int,
        temperature: float,
        retries: int = 0,
        max_retries: int = 0,
    ) -> ChatCompletion:
        try:
            return self.predict_chat_response(prompt_content)
        except Exception as e:
            if retries < max_retries:
                exponential_backoff(retries)
                return self._get_chat_completion(
                    config,
                    prompt_content,
                    model,
                    max_tokens,
                    temperature,
                    retries + 1,
                    max_retries,
                )
            else:
                print(f"Failed after {max_retries} retries.")
                raise e

    def _parse_response(self, response: str) -> Dict[str, Any]:
        try:
            response_dict = json.loads(response)
            if not isinstance(response_dict, dict):
                raise ValueError("Response is not a valid dict")
            return response_dict
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON decode error: {e}")

    def _validate_response(self, content: str, response_dict: Dict[str, Any]) -> None:
        if not self._check_list_and_dict_keys_match(
            content["column_contents"]["columns"], response_dict["columns"]
        ):
            raise ValueError("Column names do not match column contents")

    @staticmethod
    def _check_list_and_dict_keys_match(dict_list, string_list):
        if isinstance(dict_list, list):
            dict_keys = dict_list
        else:
            try:
                dict_keys = dict_list.keys()
            except:
                raise TypeError("dict_list is not a list or a dictionary")
        list_matches_keys = all(item in dict_keys for item in string_list)
        keys_match_list = all(key in string_list for key in dict_keys)
        if not (list_matches_keys and keys_match_list):
            return False
        return True


class MetadataGeneratorFactory:
    @staticmethod
    def create_generator(config) -> MetadataGenerator:
        if config.mode == "comment":
            generator = CommentGenerator()
            generator.from_context(config)
            return generator
        elif config.mode == "pi":
            generator = PIIdentifier()
            generator.from_context(config)
            return generator
        else:
            raise ValueError("Invalid mode. Use 'pi' or 'comment'.")
