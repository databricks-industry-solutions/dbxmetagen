"""A fair amount of this is stubbed out for future flexibility
around alternative API endpoints.

Need to improve NotImplementedError, deprecated marks, and other indications of stubbed out code.
"""

import os
import json
import re
import requests
import mlflow
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from openai import OpenAI
from databricks_langchain import ChatDatabricks
from databricks.sdk import WorkspaceClient
from pydantic import BaseModel
import json
from openai.types.chat.chat_completion import ChatCompletion, Choice
from openai.types.chat.chat_completion_message import ChatCompletionMessage
from requests.exceptions import HTTPError

# Models that don't support LangChain's with_structured_output()
MODELS_WITHOUT_STRUCTURED_OUTPUT = [
    "databricks-gpt-oss-120b",
    "databricks-gpt-oss-20b",
]


class ChatClient(ABC):
    """Abstract base class for different chat completion clients."""

    @abstractmethod
    def create_completion(
        self,
        messages: List[Dict[str, str]],
        model: str,
        max_tokens: int,
        temperature: float,
        **kwargs,
    ) -> Any:
        """Create a chat completion."""
        raise NotImplementedError

    @abstractmethod
    def create_structured_completion(
        self,
        messages: List[Dict[str, str]] | str,
        response_model: BaseModel,
        model: str,
        max_tokens: int,
        temperature: float,
        **kwargs,
    ) -> BaseModel:
        """Create a structured chat completion."""
        raise NotImplementedError


class DatabricksClient(ChatClient):
    """Client for Databricks native chat completions."""

    def __init__(self):
        self.openai_client = OpenAI(
            api_key=os.environ["DATABRICKS_TOKEN"],
            base_url=os.environ["DATABRICKS_HOST"] + "/serving-endpoints",
        )

    @mlflow.trace
    def create_completion(
        self,
        messages: List[Dict[str, str]],
        model: str,
        max_tokens: int,
        temperature: float,
        **kwargs,
    ) -> ChatCompletion:
        """Create a chat completion using OpenAI client with Databricks endpoint."""

        response = self.openai_client.chat.completions.create(
            messages=messages,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            **kwargs,
        )

        token_usage = self._extract_and_log_usage(
            response, model, messages, max_tokens, temperature
        )

        if hasattr(response, "__dict__"):
            response.token_usage = token_usage

        return response

    def _extract_and_log_usage(
        self,
        response: ChatCompletion,
        model: str,
        messages: List[Dict[str, str]],
        max_tokens: int,
        temperature: float,
    ) -> dict:
        """Extract token usage and log to MLFlow for benchmarking."""

        usage_info = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

        if hasattr(response, "usage") and response.usage:
            usage_info.update(
                {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                }
            )

        try:
            mlflow.log_metrics(
                {
                    "prompt_tokens": usage_info["prompt_tokens"],
                    "completion_tokens": usage_info["completion_tokens"],
                    "total_tokens": usage_info["total_tokens"],
                }
            )

            mlflow.log_params(
                {
                    "model": model,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "num_messages": len(messages),
                }
            )

            # Log text lengths for additional context
            total_prompt_length = sum(len(msg.get("content", "")) for msg in messages)
            mlflow.log_metrics(
                {
                    "total_prompt_length": total_prompt_length,
                    "chars_per_prompt_token": total_prompt_length
                    / max(usage_info["prompt_tokens"], 1),
                }
            )

        except Exception as e:
            print(f"Warning: Failed to log to MLFlow: {e}")

        return usage_info

    def get_token_usage(self, response: ChatCompletion) -> dict:
        """Utility method to get token usage from a response."""
        if hasattr(response, "token_usage"):
            return response.token_usage

        if hasattr(response, "usage") and response.usage:
            return {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
                "response_time_seconds": 0,
            }

        return {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "response_time_seconds": 0,
        }

    def _parse_text_to_structured_output(
        self, response_text: Any, response_model: BaseModel
    ) -> BaseModel:
        """Parse text response to structured output.

        Handles special AIMessage content format from models like databricks-gpt-oss-120b
        which return content as a list with reasoning and text objects.
        """
        # Handle AIMessage content which can be a list of dicts
        if isinstance(response_text, list):
            for item in response_text:
                if isinstance(item, dict) and item.get("type") == "text":
                    response_text = item.get("text", "")
                    break

        # If still not a string, try to extract from content field
        if not isinstance(response_text, str):
            if hasattr(response_text, "content"):
                response_text = response_text.content
                # Recursively handle if content is also a list
                if isinstance(response_text, list):
                    return self._parse_text_to_structured_output(
                        response_text, response_model
                    )
            else:
                response_text = str(response_text)

        # Extract JSON from the response text
        json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
        if json_match:
            response_text = json_match.group()

        try:
            response_dict = json.loads(response_text)
            return response_model(**response_dict)
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            print(f"Warning: Failed to parse structured response: {e}")
            print(f"Raw response: {response_text}")
            raise ValueError(
                f"Could not parse response into {response_model.__name__}: {e}"
            ) from e

    def create_structured_completion(
        self,
        messages: List[Dict[str, str]] | str,
        response_model: BaseModel,
        model: str,
        max_tokens: int,
        temperature: float,
        **kwargs,
    ) -> BaseModel:
        """Create a structured chat completion using ChatDatabricks.

        Automatically detects models that don't support structured outputs
        and falls back to text parsing for those models.
        """
        # Check if model requires text parsing instead of structured output
        use_text_parsing = model in MODELS_WITHOUT_STRUCTURED_OUTPUT

        try:
            if use_text_parsing:
                # Use invoke without structured output, then parse the response
                response = ChatDatabricks(
                    endpoint=model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    max_retries=1,
                ).invoke(messages)

                # Parse the response content to structured output
                return self._parse_text_to_structured_output(
                    response.content, response_model
                )
            else:
                # Use structured output for models that support it
                return (
                    ChatDatabricks(
                        endpoint=model,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        max_retries=1,
                    )
                    .with_structured_output(response_model)
                    .invoke(messages)
                )
        except HTTPError as e:
            # Check if this is the specific error about model output format
            if "Model output is not in expected format" in str(e):
                print("\n" + "=" * 80)
                print(f"WARNING: Model '{model}' does not support structured outputs.")
                print(
                    "This model should be added to MODELS_WITHOUT_STRUCTURED_OUTPUT in chat_client.py"
                )
                print("=" * 80 + "\n")
            raise


class OpenAISpecClient(ChatClient):
    """Client for OpenAI-compatible endpoints."""

    def __init__(self, base_url: str, api_key: str):
        self.openai_client = OpenAI(
            api_key=api_key,
            base_url=base_url,
        )

    def create_completion(
        self,
        messages: List[Dict[str, str]],
        model: str,
        max_tokens: int,
        temperature: float,
        **kwargs,
    ) -> ChatCompletion:
        """Create a chat completion using OpenAI-compatible endpoint."""
        return self.openai_client.chat.completions.create(
            messages=messages,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            **kwargs,
        )

    def create_structured_completion(
        self,
        messages: List[Dict[str, str]] | str,
        response_model: BaseModel,
        model: str,
        max_tokens: int,
        temperature: float,
        **kwargs,
    ) -> BaseModel:
        """Create a structured completion with JSON parsing for OpenAI-compatible endpoints."""
        if isinstance(messages, list) and messages:
            last_message = messages[-1].get("content", "")
            if "JSON" not in last_message and "json" not in last_message:
                messages = messages.copy()  # Don't modify the original
                messages[-1] = {
                    **messages[-1],
                    "content": messages[-1]["content"]
                    + "\n\nPlease respond with a valid JSON object that matches the required schema.",
                }

        completion = self.create_completion(
            messages, model, max_tokens, temperature, **kwargs
        )

        try:
            response_text = completion.choices[0].message.content
            # Try to extract JSON from the response (in case there's extra text)

            json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
            if json_match:
                response_text = json_match.group()

            response_dict = json.loads(response_text)
            return response_model(**response_dict)
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            print(f"Warning: Failed to parse structured response: {e}")
            print(f"Raw response: {response_text}")
            if (
                hasattr(response_model, "model_fields")
                and "column_contents" in response_model.model_fields
            ):
                return response_model(
                    table="unknown", columns=[], column_contents=response_text
                )
            raise ValueError(
                "Could not parse response into %s: %s", response_model.__name__, e
            )
        except Exception as e:
            raise ValueError("Unexpected error parsing structured response: %s", e)


class CustomChatSpecClient(ChatClient):
    """Client for custom chat endpoints that use 'engine' instead of 'model'."""

    def __init__(self, base_url: str, api_key: str):
        self.openai_client = OpenAI(
            api_key=api_key,
            base_url=base_url,
        )
        raise NotImplementedError("CustomChatSpecClient is not yet fully implemented")

    def create_completion(
        self,
        messages: List[Dict[str, str]],
        model: str,
        max_tokens: int,
        temperature: float,
        **kwargs,
    ) -> ChatCompletion:
        """Create a chat completion using custom endpoint with 'engine' parameter."""
        # Prepare the request payload with 'engine' instead of 'model'
        payload = {
            "engine": model,  # Use 'engine' instead of 'model'
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            **kwargs,
        }

        headers = {
            "Authorization": f"Bearer {self.openai_client.api_key}",
            "Content-Type": "application/json",
        }

        # Make raw HTTP request to the custom endpoint
        response = requests.post(
            f"{self.openai_client.base_url}/chat/completions",
            headers=headers,
            json=payload,
            timeout=60,
        )

        if not response.ok:
            raise Exception(
                f"API request failed: {response.status_code} {response.text}"
            )

        response_data = response.json()

        # Convert response to OpenAI ChatCompletion format
        choices = []
        for choice_data in response_data.get("choices", []):
            message = ChatCompletionMessage(
                role=choice_data["message"]["role"],
                content=choice_data["message"]["content"],
            )
            choice = Choice(
                index=choice_data["index"],
                message=message,
                finish_reason=choice_data.get("finish_reason"),
            )
            choices.append(choice)

        return ChatCompletion(
            id=response_data.get("id", "custom"),
            object=response_data.get("object", "chat.completion"),
            created=response_data.get("created", 0),
            model=response_data.get("model", model),
            choices=choices,
            usage=response_data.get("usage"),
        )

    def create_structured_completion(
        self,
        messages: List[Dict[str, str]] | str,
        response_model: BaseModel,
        model: str,
        max_tokens: int,
        temperature: float,
        **kwargs,
    ) -> BaseModel:
        """Create a structured completion with JSON parsing for custom endpoints."""
        if isinstance(messages, list) and messages:
            last_message = messages[-1].get("content", "")
            if "JSON" not in last_message and "json" not in last_message:
                messages = messages.copy()
                messages[-1] = {
                    **messages[-1],
                    "content": messages[-1]["content"]
                    + "\n\nPlease respond with a valid JSON object that matches the required schema.",
                }

        completion = self.create_completion(
            messages, model, max_tokens, temperature, **kwargs
        )

        # Parse the JSON response
        try:
            response_text = completion.choices[0].message.content
            # Try to extract JSON from the response (in case there's extra text)

            json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
            if json_match:
                response_text = json_match.group()

            response_dict = json.loads(response_text)
            return response_model(**response_dict)
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            print(f"Warning: Failed to parse structured response: {e}")
            print(f"Raw response: {response_text}")
            if (
                hasattr(response_model, "model_fields")
                and "column_contents" in response_model.model_fields
            ):
                # This is likely a CommentResponse or similar
                return response_model(
                    table="unknown", columns=[], column_contents=response_text
                )
            else:
                # Generic fallback - this may still fail but provides better error info
                raise ValueError(
                    "Could not parse response into {response_model.__name__}: {e}"
                )
        except Exception as e:
            raise ValueError(f"Unexpected error parsing structured response: {e}")


class ChatClientFactory:
    """Factory class to create appropriate chat clients based on configuration."""

    @staticmethod
    def create_client(config) -> ChatClient:
        """Create a chat client based on the configuration."""
        chat_type = getattr(config, "chat_completion_type", "databricks")

        if chat_type == "databricks":
            return DatabricksClient()

        elif chat_type == "openai_spec":
            if not config.custom_endpoint_url:
                raise ValueError("custom_endpoint_url is required for openai_spec type")

            api_key = ChatClientFactory._get_secret_from_scope(
                config.custom_endpoint_secret_scope, config.custom_endpoint_secret_key
            )
            return OpenAISpecClient(config.custom_endpoint_url, api_key)

        elif chat_type == "custom_chat_spec":
            if not config.custom_endpoint_url:
                raise ValueError(
                    "custom_endpoint_url is required for custom_chat_spec type"
                )

            api_key = ChatClientFactory._get_secret_from_scope(
                config.custom_endpoint_secret_scope, config.custom_endpoint_secret_key
            )
            return CustomChatSpecClient(config.custom_endpoint_url, api_key)

        else:
            raise ValueError(f"Unknown chat completion type: {chat_type}")

    @staticmethod
    def _get_secret_from_scope(scope: str, key: str) -> str:
        """Retrieve secret from Databricks secret scope."""
        if not scope or not key:
            raise ValueError(
                "Both custom_endpoint_secret_scope and custom_endpoint_secret_key are required for custom endpoints"
            )

        try:
            w = WorkspaceClient()
            secret_value = w.secrets.get_secret(scope=scope, key=key)
            return secret_value.value
        except Exception as e:
            raise ValueError(
                f"Failed to retrieve secret from scope '{scope}' with key '{key}': {e}"
            )
