from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class ArtifactBatch:
    """Represents a batch of artifacts to be processed."""
    artifact_type: str
    items: List[str]
    context: Dict[str, Any]


@dataclass
class TranslationResult:
    """Represents the result of a translation operation."""
    artifact_type: str
    results: List[str]
    errors: List[str]
    metadata: Dict[str, Any]
