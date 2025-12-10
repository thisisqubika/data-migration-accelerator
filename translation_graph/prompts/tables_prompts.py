from . import PromptBase


class TablesPrompts(PromptBase):
    """Prompts for table translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake tables to Databricks tables.

Your task is to generate Databricks table creation DDL from Snowflake table metadata structures.

METADATA STRUCTURE:
The table metadata will include the following key fields:
- database_name: The Snowflake database name (maps to Databricks CATALOG)
- schema_name: The Snowflake schema name (maps to Databricks SCHEMA)
- table_name: The table name
- table_type: The table type (BASE TABLE, EXTERNAL TABLE, etc.)
- columns: Array of column definitions with data types, nullability, defaults, and comments
- comment: Optional table-level comment

CRITICAL NAMING REQUIREMENT:
You MUST construct the fully qualified table name using the three-level namespace:
  <database_name>.<schema_name>.<table_name>

For example, if database_name is "DATA_MIGRATION_DB", schema_name is "DATA_MIGRATION_SCHEMA", 
and table_name is "CUSTOMERS", the CREATE TABLE statement MUST use:
  CREATE TABLE DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.CUSTOMERS

Key mappings:
- Snowflake PERMANENT TABLE → Databricks MANAGED TABLE (Direct Equivalent)
- Snowflake TEMPORARY TABLE → Databricks TEMPORARY VIEW (Direct Equivalent)
- Snowflake TRANSIENT TABLE → Databricks MANAGED TABLE with reduced Time Travel (Imperfect match - optional)
- Snowflake EXTERNAL TABLE → Databricks EXTERNAL TABLE (Direct Equivalent)

Important considerations for DDL generation:
- Convert Snowflake data types to Databricks equivalents:
  * NUMBER(precision, scale) → DECIMAL(precision, scale) or BIGINT/INT
  * TEXT/VARCHAR(length) → STRING (always use STRING, not VARCHAR)
  * BOOLEAN → BOOLEAN
  * TIMESTAMP_NTZ → TIMESTAMP
  * TIMESTAMP_LTZ → TIMESTAMP
  * VARIANT → STRING (or MAP/ARRAY for complex data)
  * ARRAY → ARRAY<STRING>
  * OBJECT → MAP<STRING, STRING>
- Handle nullability: "YES"/"NO" → NULL/NOT NULL
- Include column defaults where applicable
- Add table comments using COMMENT clause
- Always include USING DELTA to create Delta tables
- Generate proper CREATE TABLE syntax for Databricks
- Always use the fully qualified three-level namespace from the metadata

CRITICAL: All tables MUST include "USING DELTA" clause to ensure they are Delta Lake tables.
Example: CREATE TABLE catalog.schema.table_name (...) USING DELTA COMMENT '...'

For each table metadata object, generate the equivalent Databricks CREATE TABLE statement.

Context: {context}
Table Metadata: {table_metadata}

Provide only the generated CREATE TABLE SQL statement, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create table translation system prompt."""
        return cls.system_prompt(**kwargs)
