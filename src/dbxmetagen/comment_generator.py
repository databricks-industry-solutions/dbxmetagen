import mlflow
from src.dbxmetagen.metadata_generator import CommentGenerator
from src.dbxmetagen.chat_client import ChatClientFactory
from src.dbxmetagen.config import MetadataConfig


class CommentGeneratorModel(CommentGenerator, mlflow.pyfunc.PythonModel):
    """
    This is a workaround to allow the config to be passed as a MetadataConfig object,
    which is not supported by the chat client.
    """

    def __init__(self, config: MetadataConfig):
        self.chat_client = None
        self.config = config

    def load_context(self, context):
        pass

    def predict(self, model_input):
        """
        This is a workaround to allow the config to be passed as a MetadataConfig object,
        which is not supported by the chat client.
        """
        if not isinstance(self.config, dict):
            self.config = self.config.__dict__

        # Convert dict back to MetadataConfig-like object for chat client
        class TempConfig:
            """
            This is a workaround to allow the config to be passed as a MetadataConfig object,
            which is not supported by the chat client.
            """

            def __init__(self, config_dict):
                for key, value in config_dict.items():
                    setattr(self, key, value)

        temp_config = TempConfig(self.config)
        if not hasattr(self, "chat_client"):
            self.chat_client = ChatClientFactory.create_client(temp_config)

        self.chat_response = self.chat_client.create_completion(
            messages=model_input,
            model=self.config["model"],
            max_tokens=self.config["max_tokens"],
            temperature=self.config["temperature"],
        )
        return self.chat_response
