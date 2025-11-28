import os
import json
from typing import List, Optional, Dict, Any
from utils.types import ArtifactBatch


def determine_artifact_type_from_filename(filename: str) -> Optional[str]:
    """
    Determine artifact type from filename.

    Args:
        filename: The name of the file (with or without path)

    Returns:
        Artifact type string or None if cannot be determined
    """
    basename = os.path.basename(filename).lower()

    # Map filename keywords to artifact types
    artifact_type_mapping = {
        "tables": ["table", "tables"],
        "views": ["view", "views"],
        "schemas": ["schema", "schemas"],
        "databases": ["database", "databases", "db"],
        "procedures": ["procedure", "procedures", "proc", "procs"],
        "roles": ["role", "roles"],
        "stages": ["stage", "stages"],
        "streams": ["stream", "streams"],
        "pipes": ["pipe", "pipes"],
        "grants": ["grant", "grants"],
        "tags": ["tag", "tags"],
        "comments": ["comment", "comments"],
        "masking_policies": ["masking_policy", "masking_policies", "masking", "policy"],
        "udfs": ["udf", "udfs", "function", "functions"],
        "sequences": ["sequence", "sequences"],
        "file_formats": ["file_format", "file_formats", "format", "formats"],
        "external_locations": ["external_location", "external_locations", "external"]
    }

    for artifact_type, keywords in artifact_type_mapping.items():
        for keyword in keywords:
            if keyword in basename:
                return artifact_type

    return None


def get_json_key_mapping():
    """Get mapping from artifact types to JSON keys in files."""
    return {
        "udfs": "functions"  # Snowflake uses "functions" but we call them "udfs"
    }
    
    for artifact_type, keywords in artifact_type_mapping.items():
        for keyword in keywords:
            if keyword in basename:
                return artifact_type
    
    return None


def load_json_file(filepath: str) -> Dict[str, Any]:
    """
    Load and parse a JSON file.
    
    Args:
        filepath: Path to the JSON file
        
    Returns:
        Parsed JSON data as a dictionary
        
    Raises:
        json.JSONDecodeError: If the file is not valid JSON
        FileNotFoundError: If the file doesn't exist
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_artifacts_from_json(
    json_data: Dict[str, Any],
    artifact_type: str
) -> List[str]:
    """
    Extract artifacts from JSON data based on artifact type.
    
    Args:
        json_data: Parsed JSON data
        artifact_type: Type of artifact to extract (e.g., "tables", "views")
        
    Returns:
        List of JSON strings, each representing one artifact object
        
    Raises:
        KeyError: If the artifact type key is not found in JSON
    """
    if artifact_type not in json_data:
        raise KeyError(
            f"Artifact type '{artifact_type}' not found in JSON. "
            f"Available keys: {list(json_data.keys())}"
        )
    
    artifacts = json_data[artifact_type]
    
    if not isinstance(artifacts, list):
        raise ValueError(
            f"Expected '{artifact_type}' to be a list, got {type(artifacts).__name__}"
        )
    
    return [json.dumps(artifact) for artifact in artifacts]


def create_batches_from_file(
    filepath: str,
    batch_size: int = 10,
    context: Optional[dict] = None
) -> List[ArtifactBatch]:
    """
    Read a JSON file, determine artifact type from filename, and create batches.
    
    Args:
        filepath: Path to the JSON file
        batch_size: Number of artifacts per batch
        context: Optional context dictionary to include in batches
        
    Returns:
        List of ArtifactBatch objects
        
    Raises:
        ValueError: If artifact type cannot be determined from filename
        KeyError: If the artifact type key is not found in JSON
        json.JSONDecodeError: If the file is not valid JSON
    """
    artifact_type = determine_artifact_type_from_filename(filepath)
    
    if artifact_type is None:
        raise ValueError(
            f"Cannot determine artifact type from filename: {filepath}. "
            f"Filename should contain one of: tables, views, schemas, databases, "
            f"procedures, roles, stages, streams, pipes, grants, tags, comments, "
            f"masking_policies, udfs, sequences, file_formats, external_locations"
        )
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    json_data = load_json_file(filepath)

    # Use the correct JSON key for this artifact type
    json_key_mapping = get_json_key_mapping()
    json_key = json_key_mapping.get(artifact_type, artifact_type)
    artifact_items = extract_artifacts_from_json(json_data, json_key)
    
    if context is None:
        context = {}
    
    context["source_file"] = filepath
    
    batches = []
    total_batches = (len(artifact_items) + batch_size - 1) // batch_size
    
    for i in range(0, len(artifact_items), batch_size):
        batch_items = artifact_items[i:i + batch_size]
        batch = ArtifactBatch(
            artifact_type=artifact_type,
            items=batch_items,
            context={
                **context,
                "batch_index": i // batch_size,
                "total_batches": total_batches
            }
        )
        batches.append(batch)
    
    return batches


def process_files(
    filepaths: List[str],
    batch_size: int = 10,
    context: Optional[dict] = None
) -> List[ArtifactBatch]:
    """
    Process multiple JSON files and create batches for each.
    
    Args:
        filepaths: List of JSON file paths to process
        batch_size: Number of artifacts per batch
        context: Optional context dictionary to include in batches
        
    Returns:
        List of ArtifactBatch objects from all files
    """
    all_batches = []
    
    for filepath in filepaths:
        batches = create_batches_from_file(filepath, batch_size, context)
        all_batches.extend(batches)
    
    return all_batches

