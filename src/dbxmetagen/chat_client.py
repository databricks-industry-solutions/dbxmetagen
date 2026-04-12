"""A fair amount of this is stubbed out for future flexibility
around alternative API endpoints.

Need to improve NotImplementedError, deprecated marks, and other indications of stubbed out code.
"""

import os
import json
import logging
import re
import requests
import mlflow
import time
from abc import ABC, abstractmethod
import copy
from typing import List, Dict, Any, Type
from openai import OpenAI
from databricks_langchain import ChatDatabricks
from databricks.sdk import WorkspaceClient
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)
from openai.types.chat.chat_completion import ChatCompletion, Choice
from openai.types.chat.chat_completion_message import ChatCompletionMessage


# ---------------------------------------------------------------------------
# Structured output with automatic fallback
# ---------------------------------------------------------------------------


def _extract_and_validate_json(text: str, response_model: Type[BaseModel]) -> BaseModel:
    """Extract JSON from LLM text and validate with Pydantic."""
    original_len = len(text)
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    # 1. Try direct parse (covers well-formed responses)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = None

    if data is not None:
        try:
            return response_model.model_validate(data)
        except ValidationError as e:
            raise ValueError(
                f"JSON did not match {response_model.__name__} schema: {e}"
            ) from e

    # 2. Use raw_decode from first { or [ (handles prose-wrapped or trailing text)
    decoder = json.JSONDecoder()
    last_validation_err = None
    for start_char in ("{", "["):
        idx = text.find(start_char)
        if idx != -1:
            try:
                data, _ = decoder.raw_decode(text, idx)
            except json.JSONDecodeError:
                continue
            try:
                return response_model.model_validate(data)
            except ValidationError as e:
                last_validation_err = e
                continue

    if last_validation_err:
        raise ValueError(
            f"JSON did not match {response_model.__name__} schema: {last_validation_err}"
        ) from last_validation_err

    raise ValueError(
        f"Could not extract valid JSON for {response_model.__name__} "
        f"(response length: {original_len} chars)\n"
        f"Raw text (first 500 chars): {text[:500]}"
    )


def _append_json_instruction(
    messages: List[Dict[str, str]], response_model: Type[BaseModel],
) -> List[Dict[str, str]]:
    """Append a JSON schema instruction to the last user message (non-mutating)."""
    if not messages:
        return messages
    last_content = messages[-1].get("content", "")
    if "JSON" in last_content or "json" in last_content:
        return messages
    schema = json.dumps(response_model.model_json_schema(), indent=2)
    msgs = copy.deepcopy(messages)
    msgs[-1] = {
        **msgs[-1],
        "content": last_content
        + f"\n\nRespond ONLY with a valid JSON object matching this schema:\n{schema}",
    }
    return msgs


def invoke_structured(
    endpoint: str,
    messages: List[Dict[str, str]],
    response_model: Type[BaseModel],
    temperature: float = 0.0,
    max_tokens: int = 2048,
    max_retries: int = 1,
) -> BaseModel:
    """Invoke an LLM with structured output, falling back to JSON parsing + Pydantic.

    Tries ``ChatDatabricks.with_structured_output`` first (tool-calling path).
    If that raises (e.g. model doesn't support tool calling), falls back to a
    plain ``invoke`` with a JSON instruction appended, then parses and validates
    the response with ``response_model.model_validate``.
    """
    llm = ChatDatabricks(
        endpoint=endpoint, temperature=temperature,
        max_tokens=max_tokens, max_retries=max_retries,
    )
    try:
        return llm.with_structured_output(response_model).invoke(messages)
    except Exception as exc:
        logger.warning(
            "with_structured_output failed for %s (%s), falling back to JSON parsing",
            endpoint, exc,
        )
        print(
            f"[structured_output] with_structured_output failed for {endpoint} "
            f"({type(exc).__name__}), falling back to JSON parsing"
        )
        msgs = _append_json_instruction(messages, response_model)
        raw = llm.invoke(msgs)
        content = raw.content if hasattr(raw, "content") else str(raw)
        if isinstance(content, list):
            text = "\n".join(
                part["text"] if isinstance(part, dict) and "text" in part else str(part)
                for part in content
            )
        else:
            text = str(content)
        try:
            return _extract_and_validate_json(text, response_model)
        except ValueError:
            print(
                f"[structured_output] JSON fallback also failed for {endpoint} "
                f"(response length: {len(text)} chars, max_tokens: {max_tokens}). "
                f"If response looks truncated, increase max_tokens."
            )
            raise


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
        api_key = os.environ.get("DATABRICKS_TOKEN")
        base_url = os.environ.get("DATABRICKS_HOST")

        if not api_key or not base_url:
            try:
                w = WorkspaceClient()
                if not base_url:
                    base_url = w.config.host.rstrip("/")
                if not api_key:
                    headers = w.config.authenticate()
                    api_key = headers.get("Authorization", "").removeprefix("Bearer ")
                    logger.info("Using SDK-based authentication (no PAT in environment)")
            except Exception as e:
                logger.warning("SDK authentication fallback failed: %s", e)

        if not api_key:
            raise ValueError(
                "Could not obtain Databricks auth token. "
                "DATABRICKS_TOKEN not found in environment and SDK auth fallback failed. "
                "If PAT is disabled, ensure WorkspaceClient() can authenticate "
                "(e.g. via notebook-native auth or service principal credentials)."
            )
        if not base_url:
            raise ValueError(
                "Could not determine Databricks host. "
                "Set DATABRICKS_HOST or ensure WorkspaceClient() can resolve it."
            )

        self.openai_client = OpenAI(
            api_key=api_key,
            base_url=base_url + "/serving-endpoints",
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

    def create_structured_completion(
        self,
        messages: List[Dict[str, str]] | str,
        response_model: BaseModel,
        model: str,
        max_tokens: int,
        temperature: float,
        **kwargs,
    ) -> BaseModel:
        """Create a structured chat completion using ChatDatabricks."""
        return invoke_structured(
            model, messages, response_model,
            temperature=temperature, max_tokens=max_tokens, max_retries=1,
        )


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
            logger.warning("Failed to parse structured response: %s", e)
            logger.debug("Raw response: %s", response_text[:500])
            if (
                hasattr(response_model, "model_fields")
                and "column_contents" in response_model.model_fields
            ):
                logger.warning(
                    "Returning empty metadata for table due to parse failure "
                    "(model=%s)", response_model.__name__,
                )
                return response_model(
                    table="unknown", columns=[], column_contents=[]
                )
            raise ValueError(
                f"Could not parse response into {response_model.__name__}: {e}"
            )
        except Exception as e:
            raise ValueError(f"Unexpected error parsing structured response: {e}")


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
            logger.warning("Failed to parse structured response: %s", e)
            logger.debug("Raw response: %s", response_text[:500])
            if (
                hasattr(response_model, "model_fields")
                and "column_contents" in response_model.model_fields
            ):
                logger.warning(
                    "Returning empty metadata for table due to parse failure "
                    "(model=%s)", response_model.__name__,
                )
                return response_model(
                    table="unknown", columns=[], column_contents=[]
                )
            raise ValueError(
                f"Could not parse response into {response_model.__name__}: {e}"
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
