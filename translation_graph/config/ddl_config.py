import os
from typing import Dict, Any, Optional
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv is optional
    pass


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
        "environment": "development",
        "debug": False,
        "llms": {
            "smart_router": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.1,
                "max_tokens": 2000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "database_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "schemas_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "tables_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "views_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "stages_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "streams_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "pipes_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "roles_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "grants_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "tags_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "comments_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "masking_policies_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "udfs_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "procedures_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "sequences_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "file_formats_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "external_locations_translator": {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            },
            "evaluator": {
                "provider": "databricks",
                "model": "databricks-meta-llama-3-1-8b-instruct",
                "temperature": 0.1,
                "max_tokens": 1000,
                "additional_params": {
                    "endpoint": "databricks-meta-llama-3-1-8b-instruct"
                }
            }
        },
        "processing": {
            "batch_size": 10,
            "max_concurrent_batches": 5,
            "timeout_seconds": 300,
            "evaluation_batch_size": 5
        },
        "validation": {
            "enabled": True,
            "report_all_results": False,  # If true, reports both compliant and non-compliant statements
            "skip_unsupported_artifacts": [],  # Artifacts to skip validation entirely
            "llm_validated_artifacts": ["procedures", "pipes"],  # Artifacts that use LLM validation instead of SQLGlot
            "persist_compliant_batches": False  # Whether to persist batches with all compliant statements
        },
        "output": {
            "format": "json",
            "include_metadata": True,
            "compress_output": False,
            "base_dir": "./ddl_output",
            "timestamp_format": "%Y%m%d_%H%M%S"
        },
        "observability": {
            "log_level": "INFO",
            "log_file": None,
            "enable_metrics": True,
            "retry": {
                "max_retries": 3,
                "retry_delay": 1.0,
                "backoff_factor": 2.0
            }
        }
    }

    ENV_MAPPINGS = {
        "DDL_ENV": "environment",
        "DDL_DEBUG": ("debug", lambda x: x.lower() in ('true', '1', 'yes', 'on')),
        "DBX_ENDPOINT": ["llms", "smart_router", "additional_params", "endpoint"],
        "DDL_BATCH_SIZE": ("processing.batch_size", int),
        "DDL_MAX_CONCURRENT": ("processing.max_concurrent_batches", int),
        "DDL_TIMEOUT": ("processing.timeout_seconds", int),
        "DDL_OUTPUT_FORMAT": "output.format",
        "DDL_OUTPUT_DIR": "output.base_dir",
        "DDL_INCLUDE_METADATA": ("output.include_metadata", lambda x: x.lower() in ('true', '1', 'yes', 'on')),
        "DDL_COMPRESS_OUTPUT": ("output.compress_output", lambda x: x.lower() in ('true', '1', 'yes', 'on')),
        "DDL_LOG_LEVEL": "observability.log_level",
        "DDL_LOG_FILE": "observability.log_file",
        "DDL_MAX_RETRIES": ("observability.retry.max_retries", int),
        "DDL_RETRY_DELAY": ("observability.retry.retry_delay", float),
        "DDL_VALIDATION_ENABLED": ("validation.enabled", lambda x: x.lower() in ('true', '1', 'yes', 'on')),
        "DDL_VALIDATION_REPORT_ALL": ("validation.report_all_results", lambda x: x.lower() in ('true', '1', 'yes', 'on')),
        "DDL_VALIDATION_SKIP_UNSUPPORTED": ("validation.skip_unsupported_artifacts", lambda x: x.split(',') if x else []),
        "DDL_COMPRESS_OUTPUT": ("output.compress_output", lambda x: x.lower() in ('true', '1', 'yes', 'on'))
    }

    def __init__(self, config_overrides: Optional[Dict[str, Any]] = None):
        self._config = self.DEFAULT_CONFIG.copy()
        self._llm_configs = {}

        self._load_from_env()

        if config_overrides:
            self._config.update(config_overrides)

        self._build_llm_configs()

    def _load_from_env(self):
        for env_var, config_mapping in self.ENV_MAPPINGS.items():
            env_value = os.getenv(env_var)
            if env_value:
                if isinstance(config_mapping, tuple):
                    config_key, converter = config_mapping
                    try:
                        converted_value = converter(env_value)
                        self._set_nested_value(self._config, config_key, converted_value)
                    except (ValueError, TypeError):
                        pass
                else:
                    self._set_nested_value(self._config, config_mapping, env_value)

    def _set_nested_value_by_key(self, config: Dict[str, Any], key_path: str, value: Any):
        if "." in key_path:
            parts = key_path.split(".")
            current = config
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        else:
            config[key_path] = value

    def _set_nested_value(self, config: Dict[str, Any], path, value: Any):
        if isinstance(path, str):
            config[path] = value
            return
        current = config
        for key in path[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[path[-1]] = value

    def _build_llm_configs(self):
        llms_config = self._config.get("llms", {})
        for node_name, llm_dict in llms_config.items():
            self._llm_configs[node_name] = LLMConfig(**llm_dict)

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
