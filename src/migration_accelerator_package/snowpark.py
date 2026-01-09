"""
Snowpark Object Reader
Reads all database objects from Snowflake using Snowpark API.
This script reads all objects defined in snowflake_test_objects.sql
"""

import os
import json
from typing import Dict, List, Any, Optional
from snowflake.snowpark import Session
from databricks.sdk.runtime import dbutils
from migration_accelerator_package import constants
from migration_accelerator_package.artifact_readers import (
    ArtifactReaderFactory,
    TablesReader,
)
from migration_accelerator_package.constants import ArtifactType, ArtifactFileName
from migration_accelerator_package.logging_utils import get_app_logger

# Default scope name - can be overridden via SECRETS_SCOPE env var
DEFAULT_SECRETS_SCOPE = "migration-accelerator"

def get_secret(secret_name):
    """Retrieve secrets from Databricks secret scope"""
    scope = os.getenv("SECRETS_SCOPE", DEFAULT_SECRETS_SCOPE)
    try:
        return dbutils.secrets.get(scope, secret_name)
    except Exception as e:
        # Fallback to environment variables for local development
        return os.getenv(secret_name, "")


# Application logger (visible in Databricks stdout / cluster logs)
logger = get_app_logger("snowpark-reader")


# Connection parameters from environment variables
SFLKaccount = get_secret('SNOWFLAKE_ACCOUNT')
SFLKuser = get_secret('SNOWFLAKE_USER')
SFLKpass = get_secret('SNOWFLAKE_PASSWORD')

SFLKrole = os.getenv('SNOWFLAKE_ROLE', constants.SnowflakeConfig.SNOWFLAKE_ROLE.value) or "SYSADMIN"
SFLKwarehouse = os.getenv('SNOWFLAKE_WAREHOUSE', constants.SnowflakeConfig.SNOWFLAKE_WAREHOUSE.value) or "COMPUTE_WH"
SFLKdatabase = os.getenv('SNOWFLAKE_DATABASE', constants.SnowflakeConfig.SNOWFLAKE_DATABASE.value)
SFLKschema = os.getenv('SNOWFLAKE_SCHEMA', constants.SnowflakeConfig.SNOWFLAKE_SCHEMA.value)

SFLKregion = ""

# Validate required parameters
missing_params = []
if not SFLKuser:
    missing_params.append("SNOWFLAKE_USER")
if not SFLKpass:
    missing_params.append("SNOWFLAKE_PASSWORD")
if not SFLKdatabase:
    missing_params.append("SNOWFLAKE_DATABASE")
if not SFLKschema:
    missing_params.append("SNOWFLAKE_SCHEMA")

if missing_params:
    error_msg = (
        f"Missing required configuration: {', '.join(missing_params)}\n"
        f"Please set these in your cluster environment variables or .env file.\n"
        f"See env.example for reference."
    )
    logger.error(error_msg)
    raise ValueError(error_msg)

logger.info(f"Snowflake config: database={SFLKdatabase}, schema={SFLKschema}")

# Build connection parameters
connection_parameters = {
    "account": SFLKaccount,
    "user": SFLKuser,
    "role": SFLKrole,
    "password": SFLKpass,
    "warehouse": SFLKwarehouse,
    "database": SFLKdatabase,
    "schema": SFLKschema
}

# Add region if specified
if SFLKregion:
    connection_parameters["region"] = SFLKregion



class SnowparkObjectReader:
    """Class to read all Snowflake database objects using Snowpark."""
    
    def __init__(self, session: Session):
        """Initialize with a Snowpark session."""
        self.session = session
        self.database = SFLKdatabase
        self.schema = SFLKschema
        self._readers = {}
        self._initialize_readers()
    
    def _initialize_readers(self):
        """Initialize artifact readers using the factory pattern."""
        for artifact_type in ArtifactType:
            self._readers[artifact_type] = ArtifactReaderFactory.create_reader(
                artifact_type, self.session, self.database, self.schema
            )
    
    def get_tables(self) -> List[Dict[str, Any]]:
        """Get all tables in the schema."""
        return self._readers[ArtifactType.TABLES].read()
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get columns for a specific table."""
        tables_reader = self._readers[ArtifactType.TABLES]
        if isinstance(tables_reader, TablesReader):
            return tables_reader.read_columns(table_name)
        return []
    
    def get_views(self) -> List[Dict[str, Any]]:
        """Get all views in the schema."""
        return self._readers[ArtifactType.VIEWS].read()
    
    def get_procedures(self) -> List[Dict[str, Any]]:
        """Get all stored procedures in the schema."""
        return self._readers[ArtifactType.PROCEDURES].read()
    
    def get_functions(self) -> List[Dict[str, Any]]:
        """Get all user-defined functions in the schema."""
        return self._readers[ArtifactType.FUNCTIONS].read()
    
    def get_sequences(self) -> List[Dict[str, Any]]:
        """Get all sequences in the schema."""
        return self._readers[ArtifactType.SEQUENCES].read()
    
    def get_stages(self) -> List[Dict[str, Any]]:
        """Get all stages in the schema."""
        return self._readers[ArtifactType.STAGES].read()
    
    def get_file_formats(self) -> List[Dict[str, Any]]:
        """Get all file formats in the schema."""
        return self._readers[ArtifactType.FILE_FORMATS].read()
    
    def get_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks in the schema."""
        return self._readers[ArtifactType.TASKS].read()
    
    def get_streams(self) -> List[Dict[str, Any]]:
        """Get all streams in the schema."""
        return self._readers[ArtifactType.STREAMS].read()
    
    def get_pipes(self) -> List[Dict[str, Any]]:
        """Get all pipes in the schema."""
        return self._readers[ArtifactType.PIPES].read()
    
    def get_table_data(self, table_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get data from a specific table."""
        tables_reader = self._readers[ArtifactType.TABLES]
        if isinstance(tables_reader, TablesReader):
            return tables_reader.read_table_data(table_name, limit)
        return []

    def get_roles(self) -> List[Dict[str, Any]]:
        """Get all custom roles."""
        return self._readers[ArtifactType.ROLES].read()

    def get_grants_privileges(self) -> List[Dict[str, Any]]:
        """Get all privilege grants."""
        return self._readers[ArtifactType.GRANTS_PRIVILEGES].read()

    def get_grants_hierarchy(self) -> List[Dict[str, Any]]:
        """Get all role hierarchy grants."""
        return self._readers[ArtifactType.GRANTS_HIERARCHY].read()

    def get_grants_future(self) -> List[Dict[str, Any]]:
        """Get all future grants."""
        return self._readers[ArtifactType.GRANTS_FUTURE].read()
    
    def get_all_objects(self) -> Dict[str, Any]:
        """Get all database objects in one call using artifact readers."""
        logger.info("📊 Reading all Snowflake objects using Snowpark")
        
        objects = {
            'database': self.database,
            'schema': self.schema,
        }
        
        # Read all artifacts using facade pattern
        for artifact_type in ArtifactType:
            try:
                artifacts = self._readers[artifact_type].read()
                objects[artifact_type.value] = artifacts
                logger.info(f"✓ Found {len(artifacts)} {artifact_type.value}")
            except Exception as e:
                logger.warning(f"⚠ Error reading {artifact_type.value}: {str(e)[:100]}")
                objects[artifact_type.value] = []
        
        # Add column details and sample data for each table
        for table in objects[ArtifactType.TABLES.value]:
            # Handle both lowercase and uppercase keys
            table_name = table.get('table_name') or table.get('TABLE_NAME')
            if table_name:
                table['columns'] = self.get_table_columns(table_name)
                
                # Add sample data (limit to 10 rows each)
                try:
                    table['sample_data'] = self.get_table_data(table_name, limit=10)
                except Exception as e:
                    table['sample_data'] = f"Error retrieving data: {str(e)}"
            else:
                logger.warning(f"⚠ Could not find table_name in table object: {list(table.keys())}")
                table['columns'] = []
                table['sample_data'] = "Error: table_name not found"
        
        return objects
    
    def save_to_json(self, output_dir: str = None):
        """Save all objects to separate JSON files in Unity Catalog Volume, one per artifact type."""
        objects = self.get_all_objects()
        
        # Use default volume path if output_dir not specified
        if output_dir is None:
            catalog = os.environ.get("UC_CATALOG", constants.UnityCatalogConfig.CATALOG.value)
            schema = os.environ.get("UC_SCHEMA", constants.UnityCatalogConfig.SCHEMA.value)
            raw_volume = os.environ.get("UC_RAW_VOLUME", constants.UnityCatalogConfig.RAW_VOLUME.value) or "snowflake_artifacts_raw"
            
            # Validate required UC config
            if not catalog or not schema:
                missing = []
                if not catalog: missing.append("UC_CATALOG")
                if not schema: missing.append("UC_SCHEMA")
                error_msg = (
                    f"Missing required Unity Catalog configuration: {', '.join(missing)}\n"
                    f"Please set these in your cluster environment variables or .env file.\n"
                    f"See env.example for reference."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            base_volume_path = f"/Volumes/{catalog}/{schema}/{raw_volume}"
            logger.info(f"Unity Catalog volume path: {base_volume_path}")
        else:
            base_volume_path = output_dir
        
        # Metadata to include in each file
        metadata = {
            'database': objects['database'],
            'schema': objects['schema']
        }
        
        # Mapping of artifact types to file names
        artifact_file_mapping = {
            ArtifactType.TABLES: ArtifactFileName.TABLES,
            ArtifactType.VIEWS: ArtifactFileName.VIEWS,
            ArtifactType.PROCEDURES: ArtifactFileName.PROCEDURES,
            ArtifactType.FUNCTIONS: ArtifactFileName.FUNCTIONS,
            ArtifactType.SEQUENCES: ArtifactFileName.SEQUENCES,
            ArtifactType.STAGES: ArtifactFileName.STAGES,
            ArtifactType.FILE_FORMATS: ArtifactFileName.FILE_FORMATS,
            ArtifactType.TASKS: ArtifactFileName.TASKS,
            ArtifactType.STREAMS: ArtifactFileName.STREAMS,
            ArtifactType.PIPES: ArtifactFileName.PIPES,
            ArtifactType.ROLES: ArtifactFileName.ROLES,
            ArtifactType.GRANTS_PRIVILEGES: ArtifactFileName.GRANTS_PRIVILEGES,
            ArtifactType.GRANTS_HIERARCHY: ArtifactFileName.GRANTS_HIERARCHY,
            ArtifactType.GRANTS_FUTURE: ArtifactFileName.GRANTS_FUTURE
        }
        
        saved_files = []
        
        # Save each artifact type to its own file
        for artifact_type, file_name_enum in artifact_file_mapping.items():
            artifact_key = artifact_type.value
            if artifact_key in objects:
                filename = file_name_enum.value
                volume_path = f"{base_volume_path}/{filename}"
                
                # Prepare data with metadata
                artifact_data = {
                    **metadata,
                    artifact_key: objects[artifact_key]
                }
                
                # Convert to JSON string
                json_data = json.dumps(artifact_data, indent=2, default=str)
                
                # Write using dbutils
                dbutils.fs.put(volume_path, json_data, overwrite=True)
                
                saved_files.append(filename)
                logger.info(f"✓ Saved {artifact_key} to {filename}")
        
        logger.info(f"✓ Saved {len(saved_files)} artifact files to Unity Catalog Volume: {base_volume_path}")
    
    def object_exists(self, object_name: str, object_type: str = 'TABLE') -> bool:
        """Check if an object exists in the schema."""
        try:
            if object_type.upper() == 'TABLE':
                query = f"""
                SELECT COUNT(*) as cnt
                FROM information_schema.tables
                WHERE table_schema = '{self.schema}'
                AND table_name = '{object_name.upper()}'
                AND table_type = 'BASE TABLE'
                """
            elif object_type.upper() == 'VIEW':
                query = f"""
                SELECT COUNT(*) as cnt
                FROM information_schema.views
                WHERE table_schema = '{self.schema}'
                AND table_name = '{object_name.upper()}'
                """
            else:
                return False
            
            result = self.session.sql(query).collect()
            return result[0][0] > 0
        except Exception:
            return False
    
    def query_specific_objects(self):
        """Query the specific test objects from snowflake_test_objects.sql"""
        logger.info("🔍 Querying specific test objects")
        logger.info("Objects must be created first by running snowflake_test_objects.sql")
        
        results = {}
        
        # Query tables
        table_names = ['data_migration_source', 'data_migration_target']
        for table_name in table_names:
            try:
                if self.object_exists(table_name, 'TABLE'):
                    logger.info(f"Querying {table_name} table")
                    table = self.session.table(f'{self.database}.{self.schema}.{table_name}')
                    data = table.collect()
                    results[table_name] = [dict(row.as_dict()) for row in data]
                    logger.info(f"✓ Found {len(results[table_name])} rows for {table_name}")
                else:
                    logger.warning(f"⚠ {table_name} table does not exist (run snowflake_test_objects.sql to create it)")
                    results[table_name] = None
            except Exception as e:
                logger.error(f"✗ Error querying {table_name}: {str(e)[:100]}")
                results[table_name] = None
        
        # Query views
        view_names = [
            'data_migration_active_sources',
            'data_migration_summary',
            'data_migration_status_ranked',
            'data_migration_monthly_summary'
        ]
        
        for view_name in view_names:
            try:
                if self.object_exists(view_name, 'VIEW'):
                    logger.info(f"Querying {view_name} view")
                    view = self.session.table(f'{self.database}.{self.schema}.{view_name}')
                    data = view.collect()
                    results[view_name] = [dict(row.as_dict()) for row in data]
                    logger.info(f"✓ Found {len(results[view_name])} rows for {view_name}")
                else:
                    logger.warning(f"⚠ {view_name} view does not exist (run snowflake_test_objects.sql to create it)")
                    results[view_name] = None
            except Exception as e:
                error_msg = str(e)
                if "does not exist" in error_msg or "not authorized" in error_msg:
                    logger.warning(f"⚠ {view_name} view does not exist or not authorized")
                else:
                    logger.error(f"✗ Error querying {view_name}: {error_msg[:100]}")
                results[view_name] = None
        
        return results


def main():
    """Main function to demonstrate usage."""
    session = None
    try:
        logger.info("SNOWPARK OBJECT READER starting")
        logger.info(f"Connecting to Snowflake account: {SFLKaccount}")
        logger.info(f"User: {SFLKuser}")
        logger.info(f"Database: {SFLKdatabase}, Schema: {SFLKschema}")
        if SFLKwarehouse:
            logger.info(f"Warehouse: {SFLKwarehouse}")
        if SFLKregion:
            logger.info(f"Region: {SFLKregion}")
        
        # Create Snowpark session
        session = Session.builder.configs(connection_parameters).create()
        logger.info("✓ Successfully connected to Snowflake using Snowpark")
        
        # Test connection
        version = session.sql("SELECT CURRENT_VERSION()").collect()[0][0]
        logger.info(f"✓ Snowflake version: {version}")
        
        # Create reader
        reader = SnowparkObjectReader(session)
        
        # Get all objects
        objects = reader.get_all_objects()

        # Log summary
        summary = {
            'database': objects['database'],
            'schema': objects['schema'],
            'counts': {
                'tables': len(objects['tables']),
                'views': len(objects['views']),
                'procedures': len(objects['procedures']),
                'functions': len(objects['functions']),
                'sequences': len(objects['sequences']),
                'stages': len(objects['stages']),
                'file_formats': len(objects['file_formats']),
                'tasks': len(objects['tasks']),
                'streams': len(objects['streams']),
                'pipes': len(objects['pipes']),
                'roles': len(objects['roles']),
                'grants_privileges': len(objects['grants_privileges']),
                'grants_hierarchy': len(objects['grants_hierarchy']),
                'grants_future': len(objects['grants_future'])
            }
        }
        logger.info("SNOWFLAKE OBJECTS SUMMARY")
        logger.info(json.dumps(summary, indent=2))
        
        # Query specific test objects
        test_objects = reader.query_specific_objects()
        
        # Save to JSON file
        reader.save_to_json()

        logger.info("✓ All operations completed successfully")
        
    except Exception as e:
        logger.exception(f"✗ Error: {e}")
    finally:
        if session:
            session.close()
            logger.info("✓ Session closed")


if __name__ == '__main__':
    main()
