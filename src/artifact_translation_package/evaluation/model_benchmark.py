"""
Model benchmark configuration for SQL translation evaluation.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from artifact_translation_package.graph_builder import TranslationGraph
from artifact_translation_package.utils.types import ArtifactBatch
from artifact_translation_package.config.ddl_config import DDLConfig

logger = logging.getLogger(__name__)


@dataclass
class ModelConfig:
    """Configuration for a model to benchmark."""
    name: str
    endpoint: str
    temperature: float = 0.2
    max_tokens: int = 4000
    description: Optional[str] = None
    additional_params: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "endpoint": self.endpoint,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            **self.additional_params
        }


class ModelBenchmark:
    """Manages model configurations for benchmarking SQL translation."""
    
    def __init__(self, artifact_type: str):
        self.artifact_type = artifact_type
        self.model_configs: List[ModelConfig] = []
        
    def add_model_config(self, config: ModelConfig) -> "ModelBenchmark":
        """Add a model configuration."""
        self.model_configs.append(config)
        logger.info(f"Added model: {config.name}")
        return self
    
    def run_translation(self, batches: List[ArtifactBatch], model_config: ModelConfig) -> Dict[str, Any]:
        """Run translation using a specific model configuration."""
        logger.info(f"Running translation with model: {model_config.name}")
        
        config_overrides = {
            "llms": {
                f"{self.artifact_type}_translator": {
                    "provider": "databricks",
                    "temperature": model_config.temperature,
                    "max_tokens": model_config.max_tokens,
                    "additional_params": {
                        "endpoint": model_config.endpoint,
                        **model_config.additional_params
                    }
                }
            }
        }
        
        graph = TranslationGraph()
        graph.config = DDLConfig(config_overrides=config_overrides)
        
        result = graph.run_batches(batches)
        result.setdefault("metadata", {})["model_config"] = model_config.to_dict()
        result["metadata"]["artifact_type"] = self.artifact_type
        
        logger.info(f"Translation completed: {len(result.get(self.artifact_type, []))} results")
        return result
    
    def get_model_configs(self) -> List[ModelConfig]:
        return self.model_configs
    
    def __len__(self) -> int:
        return len(self.model_configs)


def create_default_model_configs() -> List[ModelConfig]:
    """Create default model configurations."""
    return [
        ModelConfig(
            name="llama-4-maverick",
            endpoint="databricks-llama-4-maverick",
            temperature=0.1,
            max_tokens=4000,
        ),
        ModelConfig(
            name="gemini-2.5-flash",
            endpoint="databricks-gemini-2-5-flash",
            temperature=0.1,
            max_tokens=4000,
        ),
    ]


def create_model_benchmark(artifact_type: str, use_defaults: bool = True) -> ModelBenchmark:
    """Factory to create a ModelBenchmark."""
    benchmark = ModelBenchmark(artifact_type)
    if use_defaults:
        for config in create_default_model_configs():
            benchmark.add_model_config(config)
    return benchmark
