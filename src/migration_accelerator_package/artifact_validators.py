"""
Artifact Validator Class 
Provides a clean interface for validating completeness and correctness of the ingested types of Snowflake artifacts.
"""
import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from snowflake.snowpark import Session
from migration_accelerator_package.constants import ArtifactType, ArtifactFileName
from databricks.sdk.runtime import *

class MetadataValidator:
    """
    Validates completeness and correctness of extracted Snowflake metadata.
    """
    def __init__(self, session: Session, volume_path: str):
        self.session = session
        self.volume_path = volume_path

    def _load_extracted(self, filename: str) -> Dict[str, Any]:
        path = f"{self.volume_path}/{filename}"
        raw = dbutils.fs.head(path, 50_000_000)
        return json.loads(raw)



    def load_all_artifacts(self) -> Dict[str, Dict[str, Any]]:
        extracted = {}
        for artifact_type in ArtifactType:
            file_enum = ArtifactFileName[artifact_type.name]
            filename = file_enum.value
            extracted[artifact_type.value] = self._load_extracted(filename)
        return extracted

    def count_snowflake_artifacts(self, artifact_type: ArtifactType, db: str, schema: str) -> int:
        if artifact_type == ArtifactType.TABLES:
            query = f"""
                SELECT COUNT(*) FROM {db}.information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_type = 'BASE TABLE'
            """
        elif artifact_type == ArtifactType.VIEWS:
            query = f"""
                SELECT COUNT(*)
                FROM {db}.information_schema.views
                WHERE table_schema = '{schema}'
            """
        elif artifact_type == ArtifactType.PROCEDURES:
            query = f"""
                SELECT COUNT(*)
                FROM {db}.information_schema.procedures
                WHERE procedure_schema = '{schema}'
            """
        elif artifact_type == ArtifactType.FUNCTIONS:
            query = f"""
                SELECT COUNT(*)
                FROM {db}.information_schema.functions
                WHERE function_schema = '{schema}'
            """
        elif artifact_type == ArtifactType.SEQUENCES:
            query = f"""
                SELECT COUNT(*)
                FROM {db}.information_schema.sequences
                WHERE sequence_schema = '{schema}'
            """
        elif artifact_type == ArtifactType.STAGES:
            query = f"SHOW STAGES IN SCHEMA {db}.{schema}"
            return len(self.session.sql(query).collect())
        elif artifact_type == ArtifactType.FILE_FORMATS:
            query = f"SHOW FILE FORMATS IN SCHEMA {db}.{schema}"
            return len(self.session.sql(query).collect())
        elif artifact_type == ArtifactType.TASKS:
            query = f"SHOW TASKS IN SCHEMA {db}.{schema}"
            return len(self.session.sql(query).collect())
        elif artifact_type == ArtifactType.STREAMS:
            query = f"SHOW STREAMS IN SCHEMA {db}.{schema}"
            return len(self.session.sql(query).collect())
        elif artifact_type == ArtifactType.PIPES:
            query = f"SHOW PIPES IN SCHEMA {db}.{schema}"
            return len(self.session.sql(query).collect())
        else:
            return 0

        return self.session.sql(query).collect()[0][0]
    
    def validate_completeness(self, extracted: Dict[str, Any], db: str, schema: str):
        completeness = {}

        for artifact_type in ArtifactType:
            snowflake_count = self.count_snowflake_artifacts(artifact_type, db, schema)
            extracted_count = len(extracted[artifact_type.value][artifact_type.value])

            coverage = (extracted_count / snowflake_count) if snowflake_count > 0 else 1.0

            completeness[artifact_type.value] = {
                "snowflake": snowflake_count,
                "extracted": extracted_count,
                "coverage_pct": round(coverage * 100, 2),
                "perfect_match": extracted_count == snowflake_count
            }

        return completeness

    def normalize_column(col: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize column metadata for comparison.
        Only keeps the essential fields needed for correctness validation.
        """
        col = {k.lower(): v for k, v in col.items()}

        return {
            "column_name": col.get("column_name"),
            "data_type": col.get("data_type"),
            "is_nullable": col.get("is_nullable"),
        }

    def validate_table_definition(self, db, schema, extracted_table: Dict[str, Any]) -> Dict[str, Any]:
        table_name = extracted_table["table_name"]

        query = f"""
        SELECT column_name, data_type, is_nullable,
            character_maximum_length, numeric_precision, numeric_scale,
            column_default, comment
        FROM {db}.information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """

        # Normalize Snowflake columns
        sf_columns_raw = [dict(row.as_dict()) for row in self.session.sql(query).collect()]
        sf_columns = [normalize_column(col) for col in sf_columns_raw]

        # Normalize extracted columns
        extracted_columns_raw = extracted_table.get("columns", [])
        extracted_columns = [normalize_column(col) for col in extracted_columns_raw]

        # Compute correctness statistics
        total = max(len(sf_columns), len(extracted_columns), 1)
        matches = sum(1 for sf, ex in zip(sf_columns, extracted_columns) if sf == ex)
        correctness_pct = round((matches / total) * 100, 2)

        return {
            "table": table_name,
            "snowflake_column_count": len(sf_columns),
            "extracted_column_count": len(extracted_columns),
            "matches": matches,
            "total_columns": total,
            "correctness_pct": correctness_pct,
            "columns_match_exactly": sf_columns == extracted_columns,
            "snowflake": sf_columns,
            "extracted": extracted_columns
        }



    def validate_view_definition(self, db, schema, extracted_view: Dict[str, Any]) -> Dict[str, Any]:
        view_name = extracted_view["view_name"]

        query = f"""
        SELECT view_definition
        FROM {db}.information_schema.views
        WHERE table_schema = '{schema}'
        AND table_name = '{view_name}'
        """
        result = self.session.sql(query).collect()
        sf_def = result[0]["VIEW_DEFINITION"] if result else ""

        extracted_def = extracted_view.get("view_definition", "")

        # Normalize whitespace for fair comparison
        def normalize(s):
            return " ".join(s.lower().strip().split())

        sf_norm = normalize(sf_def)
        ex_norm = normalize(extracted_def)

        match = sf_norm == ex_norm

        return {
            "view": view_name,
            "match": match,
            "snowflake_definition": sf_def,
            "extracted_definition": extracted_def
        }



    

