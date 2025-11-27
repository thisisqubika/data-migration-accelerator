"""
Artifact Reader Facade Classes
Provides a clean interface for reading different types of Snowflake artifacts.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
from snowflake.snowpark import Session
from migration_accelerator_package.constants import ArtifactType


class ArtifactReader(ABC):
    """Abstract base class for artifact readers."""
    
    def __init__(self, session: Session, database: str, schema: str):
        """Initialize the artifact reader."""
        self.session = session
        self.database = database
        self.schema = schema
    
    @abstractmethod
    def read(self) -> List[Dict[str, Any]]:
        """Read artifacts of this type."""
        pass
    
    def _normalize_keys(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize dictionary keys to lowercase."""
        return {k.lower(): v for k, v in row_dict.items()}
    
    def _normalize_rows(self, rows: List) -> List[Dict[str, Any]]:
        """Normalize a list of rows to dictionaries with lowercase keys."""
        return [self._normalize_keys(dict(row.as_dict())) for row in rows]


class TablesReader(ArtifactReader):
    """Reader for Snowflake tables."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all tables in the schema."""
        query = f"""
        SELECT 
            table_catalog as database_name,
            table_schema as schema_name,
            table_name,
            table_type,
            row_count,
            bytes,
            created,
            last_altered,
            comment
        FROM information_schema.tables
        WHERE table_schema = '{self.schema}'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        result = self.session.sql(query).collect()
        return [self._normalize_keys(dict(row.as_dict())) for row in result]
    
    def read_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get columns for a specific table."""
        query = f"""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default,
            comment
        FROM information_schema.columns
        WHERE table_schema = '{self.schema}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        result = self.session.sql(query).collect()
        return self._normalize_rows(result)
    
    def read_table_data(self, table_name: str, limit: int = None) -> List[Dict[str, Any]]:
        """Get data from a specific table."""
        query = f"SELECT * FROM {self.database}.{self.schema}.{table_name}"
        if limit:
            query += f" LIMIT {limit}"
        result = self.session.sql(query).collect()
        return [dict(row.as_dict()) for row in result]


class ViewsReader(ArtifactReader):
    """Reader for Snowflake views."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all views in the schema."""
        query = f"""
        SELECT 
            table_catalog as database_name,
            table_schema as schema_name,
            table_name as view_name,
            view_definition,
            created,
            comment
        FROM information_schema.views
        WHERE table_schema = '{self.schema}'
        ORDER BY view_name
        """
        result = self.session.sql(query).collect()
        return self._normalize_rows(result)


class ProceduresReader(ArtifactReader):
    """Reader for Snowflake stored procedures."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all stored procedures in the schema."""
        query = f"""
        SELECT 
            procedure_catalog as database_name,
            procedure_schema as schema_name,
            procedure_name,
            procedure_definition,
            created,
            last_altered,
            comment
        FROM information_schema.procedures
        WHERE procedure_schema = '{self.schema}'
        ORDER BY procedure_name
        """
        result = self.session.sql(query).collect()
        return self._normalize_rows(result)


class FunctionsReader(ArtifactReader):
    """Reader for Snowflake user-defined functions."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all user-defined functions in the schema."""
        query = f"""
        SELECT 
            function_catalog as database_name,
            function_schema as schema_name,
            function_name,
            function_definition,
            created,
            last_altered,
            comment
        FROM information_schema.functions
        WHERE function_schema = '{self.schema}'
        ORDER BY function_name
        """
        result = self.session.sql(query).collect()
        return self._normalize_rows(result)


class SequencesReader(ArtifactReader):
    """Reader for Snowflake sequences."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all sequences in the schema."""
        query = f"SHOW SEQUENCES IN SCHEMA {self.database}.{self.schema}"
        try:
            result = self.session.sql(query).collect()
            return self._normalize_rows(result)
        except Exception as e:
            # Fallback: try information_schema with basic columns only
            print(f"  ⚠ Warning: SHOW SEQUENCES failed, trying information_schema: {e}")
            query = f"""
            SELECT 
                sequence_catalog as database_name,
                sequence_schema as schema_name,
                sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema = '{self.schema}'
            ORDER BY sequence_name
            """
            result = self.session.sql(query).collect()
            return self._normalize_rows(result)


class StagesReader(ArtifactReader):
    """Reader for Snowflake stages."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all stages in the schema."""
        query = f"SHOW STAGES IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        return self._normalize_rows(result)


class FileFormatsReader(ArtifactReader):
    """Reader for Snowflake file formats."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all file formats in the schema."""
        query = f"SHOW FILE FORMATS IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        return self._normalize_rows(result)


class TasksReader(ArtifactReader):
    """Reader for Snowflake tasks."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all tasks in the schema."""
        query = f"SHOW TASKS IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        return self._normalize_rows(result)


class StreamsReader(ArtifactReader):
    """Reader for Snowflake streams."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all streams in the schema."""
        query = f"SHOW STREAMS IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        return self._normalize_rows(result)


class PipesReader(ArtifactReader):
    """Reader for Snowflake pipes."""
    
    def read(self) -> List[Dict[str, Any]]:
        """Get all pipes in the schema."""
        query = f"SHOW PIPES IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        return self._normalize_rows(result)


class ArtifactReaderFactory:
    """Factory for creating artifact readers."""
    
    _readers = {
        ArtifactType.TABLES: TablesReader,
        ArtifactType.VIEWS: ViewsReader,
        ArtifactType.PROCEDURES: ProceduresReader,
        ArtifactType.FUNCTIONS: FunctionsReader,
        ArtifactType.SEQUENCES: SequencesReader,
        ArtifactType.STAGES: StagesReader,
        ArtifactType.FILE_FORMATS: FileFormatsReader,
        ArtifactType.TASKS: TasksReader,
        ArtifactType.STREAMS: StreamsReader,
        ArtifactType.PIPES: PipesReader,
    }
    
    @classmethod
    def create_reader(cls, artifact_type: ArtifactType, session: Session, database: str, schema: str) -> ArtifactReader:
        """Create an artifact reader for the given type."""
        reader_class = cls._readers.get(artifact_type)
        if not reader_class:
            raise ValueError(f"No reader available for artifact type: {artifact_type}")
        return reader_class(session, database, schema)

