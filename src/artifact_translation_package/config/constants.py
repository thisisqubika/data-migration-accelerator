from enum import Enum

class UnityCatalogConfig(Enum):
    """Unity Catalog configuration for translation package"""
    CATALOG = "qubika_partner_solutions"
    SCHEMA = "migration_accelerator"
    RAW_VOLUME = "snowflake_artifacts_raw"
    

class LangGraphConfig(Enum):
    DBX_ENDPOINT= "databricks-llama-4-maverick"

    ENVIRONMENT = "development" 
    #DDL Settings
    DDL_BATCH_SIZE = 8
    # Default outputs for Databricks should go to a Volume mounted path
    # Use segmented path segments: /Volumes/<catalog>/<schema>/<volume_name>/
    DDL_OUTPUT_DIR = "/Volumes/qubika_partner_solutions/migration_accelerator/outputs"
    
    # Optional: DDL Generation Settings
    #DDL_CATALOG_NAME= "demo_catalog"
    #DDL_SCHEMA_NAME=" bronze"
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
    LANGSMITH_PROJECT="databricks-migration-accelerator-local"
    #Configured as secrets (under migration-accelerator scope):
    #LANGSMITH_ENDPOINT
    #LANGSMITH_API_KEY

    #Lakebase Settings
    LAKEBASE_DATABASE = "databricks_postgres"
    #Configured as secrets (under migration-accelerator scope):
    #LAKEBASE_HOST 
    #LAKEBASE_USER 
    #LAKEBASE_PASSWORD 

    #Secrets Settings
    SECRETS_SCOPE = "migration-accelerator"

