"""
Snowpark Object Reader
Reads all database objects from Snowflake using Snowpark API.
This script reads all objects defined in snowflake_test_objects.sql
"""

import os
import json
from typing import Dict, List, Any, Optional
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from databricks.sdk.runtime import dbutils
from migration_accelerator_package import constants

def get_secret(secret_name):
    """Retrieve secrets from Databricks secret scope"""
    try:
        return dbutils.secrets.get("migration-accelerator", secret_name)
    except Exception as e:
        # Fallback to environment variables for local development
        return os.getenv(secret_name, "")


# Connection parameters from environment variables
SFLKaccount = get_secret('SNOWFLAKE_ACCOUNT')
SFLKuser = get_secret('SNOWFLAKE_USER')
SFLKpass = get_secret('SNOWFLAKE_PASSWORD')

SFLKrole = constants.SnowflakeConfig.SNOWFLAKE_ROLE.value
SFLKwarehouse = constants.SnowflakeConfig.SNOWFLAKE_WAREHOUSE.value
SFLKdatabase = constants.SnowflakeConfig.SNOWFLAKE_DATABASE.value
SFLKschema = constants.SnowflakeConfig.SNOWFLAKE_SCHEMA.value

SFLKregion = ""

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

# Validate required parameters
if not SFLKuser or not SFLKpass:
    raise ValueError(
        "Missing required environment variables. Please set SNOWFLAKE_USER and SNOWFLAKE_PASSWORD "
        "in your .env file. See env.example for reference."
    )


class SnowparkObjectReader:
    """Class to read all Snowflake database objects using Snowpark."""
    
    def __init__(self, session: Session):
        """Initialize with a Snowpark session."""
        self.session = session
        self.database = SFLKdatabase
        self.schema = SFLKschema
    
    def get_tables(self) -> List[Dict[str, Any]]:
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
        # Convert to dict and normalize keys (handle case sensitivity)
        tables = []
        for row in result:
            row_dict = dict(row.as_dict())
            # Normalize keys to lowercase for consistency
            normalized = {k.lower(): v for k, v in row_dict.items()}
            tables.append(normalized)
        return tables
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
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
        # Normalize keys to lowercase
        return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_views(self) -> List[Dict[str, Any]]:
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
        # Normalize keys to lowercase
        return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_procedures(self) -> List[Dict[str, Any]]:
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
        # Normalize keys to lowercase
        return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_functions(self) -> List[Dict[str, Any]]:
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
        # Normalize keys to lowercase
        return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_sequences(self) -> List[Dict[str, Any]]:
        """Get all sequences in the schema."""
        # Use SHOW command for sequences as information_schema may not have all columns
        query = f"SHOW SEQUENCES IN SCHEMA {self.database}.{self.schema}"
        try:
            result = self.session.sql(query).collect()
            # Normalize keys to lowercase
            return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
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
            # Normalize keys to lowercase
            return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_stages(self) -> List[Dict[str, Any]]:
        """Get all stages in the schema."""
        # Use SHOW command for stages
        query = f"SHOW STAGES IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        # Normalize keys to lowercase
        return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_file_formats(self) -> List[Dict[str, Any]]:
        """Get all file formats in the schema."""
        query = f"SHOW FILE FORMATS IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        # Normalize keys to lowercase
        return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks in the schema."""
        query = f"SHOW TASKS IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        # Normalize keys to lowercase
        return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_streams(self) -> List[Dict[str, Any]]:
        """Get all streams in the schema."""
        query = f"SHOW STREAMS IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        # Normalize keys to lowercase
        return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_pipes(self) -> List[Dict[str, Any]]:
        """Get all pipes in the schema."""
        query = f"SHOW PIPES IN SCHEMA {self.database}.{self.schema}"
        result = self.session.sql(query).collect()
        # Normalize keys to lowercase
        return [{k.lower(): v for k, v in dict(row.as_dict()).items()} for row in result]
    
    def get_table_data(self, table_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get data from a specific table."""
        query = f"SELECT * FROM {self.database}.{self.schema}.{table_name}"
        if limit:
            query += f" LIMIT {limit}"
        result = self.session.sql(query).collect()
        return [dict(row.as_dict()) for row in result]
    
    def get_all_objects(self) -> Dict[str, Any]:
        """Get all database objects in one call."""
        print("\n📊 Reading all Snowflake objects using Snowpark...")
        
        objects = {
            'database': self.database,
            'schema': self.schema,
            'tables': self.get_tables(),
            'views': self.get_views(),
            'procedures': self.get_procedures(),
            'functions': self.get_functions(),
            'sequences': self.get_sequences(),
            'stages': self.get_stages(),
            'file_formats': self.get_file_formats(),
            'tasks': self.get_tasks(),
            'streams': self.get_streams(),
            'pipes': self.get_pipes(),
        }
        
        # Add column details for each table
        for table in objects['tables']:
            # Handle both lowercase and uppercase keys
            table_name = table.get('table_name') or table.get('TABLE_NAME')
            if table_name:
                table['columns'] = self.get_table_columns(table_name)
            else:
                print(f"  ⚠ Warning: Could not find table_name in table object: {list(table.keys())}")
                table['columns'] = []
        
        # Add sample data for tables (limit to 10 rows each)
        for table in objects['tables']:
            try:
                # Handle both lowercase and uppercase keys
                table_name = table.get('table_name') or table.get('TABLE_NAME')
                if table_name:
                    table['sample_data'] = self.get_table_data(table_name, limit=10)
                else:
                    table['sample_data'] = "Error: table_name not found"
            except Exception as e:
                table['sample_data'] = f"Error retrieving data: {str(e)}"
        
        print(f"✓ Found {len(objects['tables'])} tables")
        print(f"✓ Found {len(objects['views'])} views")
        print(f"✓ Found {len(objects['procedures'])} procedures")
        print(f"✓ Found {len(objects['functions'])} functions")
        print(f"✓ Found {len(objects['sequences'])} sequences")
        print(f"✓ Found {len(objects['stages'])} stages")
        print(f"✓ Found {len(objects['file_formats'])} file formats")
        print(f"✓ Found {len(objects['tasks'])} tasks")
        print(f"✓ Found {len(objects['streams'])} streams")
        print(f"✓ Found {len(objects['pipes'])} pipes")
        
        return objects
    
    def save_to_json(self, output_file: str = 'snowflake_objects_snowpark.json'):
        """Save all objects to a JSON file in Unity Catalog Volume using dbutils."""
        objects = self.get_all_objects()
        
        # Convert objects to JSON string
        json_data = json.dumps(objects, indent=2, default=str)
        
        # Define the volume path
        volume_path = f"/Volumes/{constants.UnityCatalogConfig.CATALOG.value}/{constants.UnityCatalogConfig.SCHEMA.value}/{constants.UnityCatalogConfig.RAW_VOLUME.value}/{output_file}"
        
        # Write using dbutils
        dbutils.fs.put(volume_path, json_data, overwrite=True)
        
        print(f"\n✓ Saved all objects to Unity Catalog Volume: {volume_path}")
    
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
        print("\n🔍 Querying specific test objects...")
        print("  (Note: Objects must be created first by running snowflake_test_objects.sql)")
        
        results = {}
        
        # Query tables
        table_names = ['data_migration_source', 'data_migration_target']
        for table_name in table_names:
            try:
                if self.object_exists(table_name, 'TABLE'):
                    print(f"  - Querying {table_name} table...")
                    table = self.session.table(f'{self.database}.{self.schema}.{table_name}')
                    data = table.collect()
                    results[table_name] = [dict(row.as_dict()) for row in data]
                    print(f"    ✓ Found {len(results[table_name])} rows")
                else:
                    print(f"  - ⚠ {table_name} table does not exist (run snowflake_test_objects.sql to create it)")
                    results[table_name] = None
            except Exception as e:
                print(f"    ✗ Error querying {table_name}: {str(e)[:100]}")
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
                    print(f"  - Querying {view_name} view...")
                    view = self.session.table(f'{self.database}.{self.schema}.{view_name}')
                    data = view.collect()
                    results[view_name] = [dict(row.as_dict()) for row in data]
                    print(f"    ✓ Found {len(results[view_name])} rows")
                else:
                    print(f"  - ⚠ {view_name} view does not exist (run snowflake_test_objects.sql to create it)")
                    results[view_name] = None
            except Exception as e:
                error_msg = str(e)
                if "does not exist" in error_msg or "not authorized" in error_msg:
                    print(f"  - ⚠ {view_name} view does not exist or not authorized")
                else:
                    print(f"    ✗ Error querying {view_name}: {error_msg[:100]}")
                results[view_name] = None
        
        return results


def main():
    """Main function to demonstrate usage."""
    session = None
    try:
        print("=" * 60)
        print("SNOWPARK OBJECT READER")
        print("=" * 60)
        print(f"Connecting to Snowflake account: {SFLKaccount}")
        print(f"User: {SFLKuser}")
        print(f"Database: {SFLKdatabase}, Schema: {SFLKschema}")
        if SFLKwarehouse:
            print(f"Warehouse: {SFLKwarehouse}")
        if SFLKregion:
            print(f"Region: {SFLKregion}")
        
        # Create Snowpark session
        session = Session.builder.configs(connection_parameters).create()
        print("✓ Successfully connected to Snowflake using Snowpark")
        
        # Test connection
        version = session.sql("SELECT CURRENT_VERSION()").collect()[0][0]
        print(f"✓ Snowflake version: {version}")
        
        # Create reader
        reader = SnowparkObjectReader(session)
        
        # Get all objects
        objects = reader.get_all_objects()
        
        # Print summary
        print("\n" + "=" * 60)
        print("SNOWFLAKE OBJECTS SUMMARY")
        print("=" * 60)
        print(json.dumps({
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
            }
        }, indent=2))
        
        # Query specific test objects
        test_objects = reader.query_specific_objects()
        
        # Save to JSON file
        reader.save_to_json()
        
        print("\n✓ All operations completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if session:
            session.close()
            print("\n✓ Session closed")


if __name__ == '__main__':
    main()
