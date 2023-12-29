import os
import orjson

from pathlib import Path
from typing import Optional, Any, Dict, Tuple
from pydantic_settings import PydanticBaseSettingsSource
from pydantic.fields import FieldInfo


class JsonConfigSettingsSource(PydanticBaseSettingsSource):
    """
    A simple settings source class that loads variables from a JSON file
    at the project's root.

    Here we happen to choose to use the `env_file_encoding` from Config
    when reading `config.json`
    """

    def get_config_file_path(self) -> Optional[Path]:
        config_file = os.environ.get("DATENSTROM_CONFIG", None)
        if not config_file:
            # try config.json in current directory
            config_file_path = os.path.join(os.getcwd(), "config.json")
            if os.path.exists(config_file_path):
                return Path(config_file_path)
            return None
        else:
            # raise error if file does not exist
            if not os.path.exists(config_file):
                raise FileNotFoundError(f"Config file {config_file} not found")
        return Path(config_file)

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        encoding = self.config.get('env_file_encoding')
        config_file_path = self.get_config_file_path()
        if not config_file_path:
            return None, field_name, False

        file_content_json = orjson.loads(
            config_file_path.read_text(encoding)
        )
        field_value = file_content_json.get(field_name)
        return field_value, field_name, False

    def prepare_field_value(
        self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool
    ) -> Any:
        return value

    def __call__(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}

        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(
                field, field_name
            )
            field_value = self.prepare_field_value(
                field_name, field, field_value, value_is_complex
            )
            if field_value is not None:
                d[field_key] = field_value

        return d