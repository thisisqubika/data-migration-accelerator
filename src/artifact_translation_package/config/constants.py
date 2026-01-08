from enum import Enum

class UnityCatalogConfig(Enum):
    """Unity Catalog configuration for translation package.
    
    These values are placeholders. Set via environment variables:
    - UC_CATALOG: Unity Catalog name (required)
    - UC_SCHEMA: Schema name (required)
    - UC_RAW_VOLUME: Volume name (optional, has default)
    """
    # Placeholders - must be overridden via env vars
    CATALOG = ""  # Set UC_CATALOG env var
    SCHEMA = ""   # Set UC_SCHEMA env var
    RAW_VOLUME = "snowflake_artifacts_raw"  # Can keep default
    

class LangGraphConfig(Enum):
    DBX_ENDPOINT= "databricks-llama-4-maverick"

    ENVIRONMENT = "development" 
    #DDL Settings
    DDL_BATCH_SIZE = 8
    # DDL_OUTPUT_DIR must be set via env var (no default - depends on catalog)
    DDL_OUTPUT_DIR = ""  # Set DDL_OUTPUT_DIR env var
    
    # Optional: DDL Generation Settings
    DDL_TEMPERATURE=0.1
    DDL_MAX_TOKENS=2000
    DDL_MAX_CONCURRENT = 5              
    DDL_TIMEOUT = 300               
    # Output Configuration
    DDL_OUTPUT_FORMAT = "sql"           
    DDL_INCLUDE_METADATA = True          
    DDL_COMPRESS_OUTPUT = False  

    # Optional: Feature flags
    DDL_ENABLE_MLFLOW=True
    DDL_VERBOSE_LOGGING=True
    DDL_DEBUG = False   

    # LangSmith Settings
    LANGSMITH_TRACING=True
    LANGSMITH_PROJECT="databricks-migration-accelerator"
    #Configured as secrets (under scope defined by SECRETS_SCOPE):
    #LANGSMITH_ENDPOINT
    #LANGSMITH_API_KEY

    #Lakebase Settings
    LAKEBASE_DATABASE = "databricks_postgres"
    #Configured as secrets:
    #LAKEBASE_HOST 
    #LAKEBASE_USER 
    #LAKEBASE_PASSWORD 

    #Secrets Settings - default scope name
    SECRETS_SCOPE = "migration-accelerator"
