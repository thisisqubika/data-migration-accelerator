import os
from typing import Dict, Any, Optional
from dataclasses import dataclass
from databricks.sdk import *
from artifact_translation_package.config.constants import LangGraphConfig
from artifact_translation_package.config.secrets import get_secret

@dataclass
class LLMConfig:
    provider: str
    model: str
    api_key: Optional[str] = None
    temperature: float = 0.7
    max_tokens: Optional[int] = None
    base_url: Optional[str] = None
    additional_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.additional_params is None:
            self.additional_params = {}


class DDLConfig:
    DEFAULT_CONFIG = {
        "environment": LangGraphConfig.ENVIRONMENT.value,
        "debug": LangGraphConfig.DDL_DEBUG.value,
        "llms": {
            "smart_router": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.1,
                "max_tokens": 2000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "database_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "schemas_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "tables_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "views_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "stages_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "streams_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "pipes_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "roles_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "grants_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "tags_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "comments_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "masking_policies_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "udfs_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "procedures_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "sequences_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "file_formats_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "evaluator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.1,
                "max_tokens": 2000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            },
            "external_locations_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": LangGraphConfig.DBX_ENDPOINT.value
                }
            }
        },
        "processing": {
            "batch_size": LangGraphConfig.DDL_BATCH_SIZE.value,
            "max_concurrent_batches": LangGraphConfig.DDL_MAX_CONCURRENT.value,
            "timeout_seconds": LangGraphConfig.DDL_TIMEOUT.value
        },
        "output": {
            "format": LangGraphConfig.DDL_OUTPUT_FORMAT.value,
            "include_metadata": LangGraphConfig.DDL_INCLUDE_METADATA.value,
            "compress_output": LangGraphConfig.DDL_COMPRESS_OUTPUT.value,
            "base_dir": LangGraphConfig.DDL_OUTPUT_DIR.value,
            "timestamp_format": "%Y%m%d_%H%M%S"
        },
        "validation": {
            "enabled": True,
            "report_all_results": False,
            "llm_validated_artifacts": ["procedures", "pipes"],
            "skip_unsupported_artifacts": ["grants", "procedures", "udfs"]
        },
        "langsmith": {
            "tracing": LangGraphConfig.LANGSMITH_TRACING.value,
            "project": LangGraphConfig.LANGSMITH_PROJECT.value,
            "endpoint": None,  # Will be loaded from secrets
            "api_key": None    # Will be loaded from secrets
        },
        "lakebase": {
            "database": LangGraphConfig.LAKEBASE_DATABASE.value,
            "host": None,      # Will be loaded from secrets
            "user": None,      # Will be loaded from secrets
            "password": None   # Will be loaded from secrets
        }
    }

    def __init__(self, config_overrides: Optional[Dict[str, Any]] = None):
        self._config = self.DEFAULT_CONFIG.copy()
        self._llm_configs = {}

        self._load_secrets()

        if config_overrides:
            self._config.update(config_overrides)

        self._build_llm_configs()

    def _load_secrets(self):
        """Load sensitive configuration from Databricks secrets"""
        # LangSmith secrets
        langsmith_endpoint = get_secret("LANGSMITH_ENDPOINT")
        if langsmith_endpoint:
            self._config["langsmith"]["endpoint"] = langsmith_endpoint
        
        langsmith_api_key = get_secret("LANGSMITH_API_KEY")
        if langsmith_api_key:
            self._config["langsmith"]["api_key"] = langsmith_api_key
        
        # Lakebase secrets
        lakebase_host = get_secret("LAKEBASE_HOST")
        if lakebase_host:
            self._config["lakebase"]["host"] = lakebase_host
        
        lakebase_user = get_secret("LAKEBASE_USER")
        if lakebase_user:
            self._config["lakebase"]["user"] = lakebase_user
        
        lakebase_password = get_secret("LAKEBASE_PASSWORD")
        if lakebase_password:
            self._config["lakebase"]["password"] = lakebase_password

    def _build_llm_configs(self):
        llms_config = self._config.get("llms", {})
        for node_name, llm_dict in llms_config.items():
            self._llm_configs[node_name] = LLMConfig(**llm_dict)
    
    def get_langsmith_config(self) -> Dict[str, Any]:
        """Get LangSmith configuration"""
        return self._config.get("langsmith", {})

    def get_lakebase_config(self) -> Dict[str, Any]:
        """Get Lakebase configuration"""
        return self._config.get("lakebase", {})

    def is_langsmith_enabled(self) -> bool:
        """Check if LangSmith tracing is enabled"""
        return self._config.get("langsmith", {}).get("tracing", False)

    def get_llm_for_node(self, node_name: str) -> LLMConfig:
        if node_name not in self._llm_configs:
            raise ValueError(f"No LLM configuration found for node: {node_name}")
        return self._llm_configs[node_name]

    def get(self, key: str, default=None) -> Any:
        return self._config.get(key, default)

    def get_nested(self, key_path: str, default=None) -> Any:
        if "." in key_path:
            parts = key_path.split(".")
            current = self._config
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return default
            return current
        return self._config.get(key_path, default)

    def update(self, updates: Dict[str, Any]):
        self._config.update(updates)

    def to_dict(self) -> Dict[str, Any]:
        return self._config.copy()


_config_instance: Optional[DDLConfig] = None


def get_config() -> DDLConfig:
    global _config_instance
    if _config_instance is None:
        _config_instance = DDLConfig()
    return _config_instance


def create_node_llm(node_name: str, config: Optional[DDLConfig] = None):
    if config is None:
        config = get_config()

    llm_config = config.get_llm_for_node(node_name)

    try:
        from databricks_langchain import ChatDatabricks
    except ImportError:
        try:
            from langchain_databricks import ChatDatabricks
        except ImportError:
            from langchain_community.chat_models import ChatDatabricks

    return ChatDatabricks(
        endpoint=llm_config.additional_params.get("endpoint", "databricks-llama-4-maverick"),
        temperature=llm_config.temperature,
        max_tokens=llm_config.max_tokens or 2000,
    )
