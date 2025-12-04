from enum import Enum

class SnowflakeConfig(Enum):
    SNOWFLAKE_ROLE = "SYSADMIN"
    SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
    SNOWFLAKE_DATABASE = "DATA_MIGRATION_DB"
    SNOWFLAKE_SCHEMA = "DATA_MIGRATION_SCHEMA"

class UnityCatalogConfig(Enum):
    CATALOG = "qubika_partner_solutions"
    SCHEMA = "migration_accelerator"
    RAW_VOLUME = "snowflake_artifacts_raw"

class ArtifactType(Enum):
    """Enumeration of Snowflake artifact types."""
    TABLES = "tables"
    VIEWS = "views"
    PROCEDURES = "procedures"
    FUNCTIONS = "functions"
    SEQUENCES = "sequences"
    STAGES = "stages"
    FILE_FORMATS = "file_formats"
    TASKS = "tasks"
    STREAMS = "streams"
    PIPES = "pipes"
    ROLES = "roles"
    GRANTS_PRIVILEGES = "grants_privileges"
    GRANTS_HIERARCHY = "grants_hierarchy"

class ArtifactFileName(Enum):
    """Enumeration of output file names for each artifact type."""
    TABLES = "tables.json"
    VIEWS = "views.json"
    PROCEDURES = "procedures.json"
    FUNCTIONS = "functions.json"
    SEQUENCES = "sequences.json"
    STAGES = "stages.json"
    FILE_FORMATS = "file_formats.json"
    TASKS = "tasks.json"
    STREAMS = "streams.json"
    PIPES = "pipelines.json"  # pipes saved as pipelines.json
    ROLES = "roles.json"
    GRANTS_PRIVILEGES = "grants_privileges.json"
    GRANTS_HIERARCHY = "grants_hierarchy.json"