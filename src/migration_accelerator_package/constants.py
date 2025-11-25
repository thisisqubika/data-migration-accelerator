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