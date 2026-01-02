"""
Evaluation dataset management for SQL translation benchmarking.
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

from artifact_translation_package.utils.types import ArtifactBatch

logger = logging.getLogger(__name__)


class EvaluationDataset:
    """Loads and prepares evaluation datasets for MLflow."""
    
    def __init__(self, artifact_type: str, source_file: Optional[str] = None, examples_dir: Optional[str] = None):
        self.artifact_type = artifact_type
        self.source_file = source_file
        self.examples_dir = examples_dir or str(Path(__file__).parent.parent / "examples")
        self.raw_data: List[Dict[str, Any]] = []
        self.dataframe: Optional[pd.DataFrame] = None
        
    def load(self) -> "EvaluationDataset":
        """Load evaluation data from JSON file."""
        filepath = self.source_file or os.path.join(self.examples_dir, f"{self.artifact_type}.json")
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Evaluation data file not found: {filepath}")
        
        logger.info(f"Loading evaluation data from: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.raw_data = self._extract_artifacts(data)
        logger.info(f"Loaded {len(self.raw_data)} {self.artifact_type} for evaluation")
        return self
    
    def _extract_artifacts(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract artifacts from JSON data."""
        if self.artifact_type in data:
            return data[self.artifact_type]
        if isinstance(data, list):
            return data
        for key in ['items', 'artifacts']:
            if key in data:
                return data[key]
        logger.warning(f"Could not find artifacts for type: {self.artifact_type}")
        return []
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert to pandas DataFrame."""
        if not self.raw_data:
            raise ValueError("No data loaded. Call load() first.")
        
        records = [
            {
                "artifact_id": f"{self.artifact_type}_{idx}",
                "artifact_type": self.artifact_type,
                "source_data": json.dumps(artifact),
            }
            for idx, artifact in enumerate(self.raw_data)
        ]
        
        self.dataframe = pd.DataFrame(records)
        return self.dataframe
    
    def to_batches(self, batch_size: int = 10) -> List[ArtifactBatch]:
        """Convert to artifact batches for translation."""
        if not self.raw_data:
            raise ValueError("No data loaded. Call load() first.")
        
        items = [json.dumps(artifact) for artifact in self.raw_data]
        batches = []
        
        for i in range(0, len(items), batch_size):
            batch = ArtifactBatch(
                artifact_type=self.artifact_type,
                items=items[i:i + batch_size],
                context={"source": "evaluation_dataset", "batch_index": len(batches)}
            )
            batches.append(batch)
        
        logger.info(f"Created {len(batches)} batches")
        return batches
    
    def __len__(self) -> int:
        return len(self.raw_data)


def load_evaluation_dataset(
    artifact_type: str,
    source_file: Optional[str] = None,
    as_dataframe: bool = True
) -> pd.DataFrame | EvaluationDataset:
    """Load and prepare evaluation dataset."""
    dataset = EvaluationDataset(artifact_type=artifact_type, source_file=source_file)
    dataset.load()
    return dataset.to_dataframe() if as_dataframe else dataset
