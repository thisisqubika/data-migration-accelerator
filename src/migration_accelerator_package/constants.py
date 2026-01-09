from enum import Enum

class SnowflakeConfig(Enum):
    """Snowflake configuration.
    
    Database and schema must be set via environment variables:
    - SNOWFLAKE_DATABASE (required)
    - SNOWFLAKE_SCHEMA (required)
    """
    SNOWFLAKE_ROLE = "SYSADMIN"
    SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
    # Placeholders - must be overridden via env vars
    SNOWFLAKE_DATABASE = ""  # Set SNOWFLAKE_DATABASE env var
    SNOWFLAKE_SCHEMA = ""    # Set SNOWFLAKE_SCHEMA env var

class UnityCatalogConfig(Enum):
    """Unity Catalog configuration.
    
    Catalog and schema must be set via environment variables:
    - UC_CATALOG (required)
    - UC_SCHEMA (required)
    """
    # Placeholders - must be overridden via env vars
    CATALOG = ""  # Set UC_CATALOG env var
    SCHEMA = ""   # Set UC_SCHEMA env var
    RAW_VOLUME = "snowflake_artifacts_raw"  # Can keep default

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
    GRANTS_FUTURE = "grants_future"

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
    GRANTS_FUTURE = "grants_future.json"
    GRANTS_FLATTENED = "grants_flattened.json"