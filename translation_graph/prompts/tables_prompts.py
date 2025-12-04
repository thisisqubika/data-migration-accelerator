from . import PromptBase


class TablesPrompts(PromptBase):
    """Prompts for table translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake tables to Databricks tables.

Your task is to generate Databricks table creation DDL from Snowflake table metadata structures.

Key mappings:
- Snowflake PERMANENT TABLE → Databricks MANAGED TABLE (Direct Equivalent)
- Snowflake TEMPORARY TABLE → Databricks TEMPORARY VIEW (Direct Equivalent)
- Snowflake TRANSIENT TABLE → Databricks MANAGED TABLE with reduced Time Travel (Imperfect match - optional)
- Snowflake EXTERNAL TABLE → Databricks EXTERNAL TABLE (Direct Equivalent)

Important considerations for DDL generation:
- Convert Snowflake data types to Databricks equivalents:
  * NUMBER(precision, scale) → DECIMAL(precision, scale) or BIGINT/INT
  * TEXT(length) → STRING or VARCHAR(length)
  * BOOLEAN → BOOLEAN
  * TIMESTAMP_NTZ → TIMESTAMP
  * TIMESTAMP_LTZ → TIMESTAMP
  * VARIANT → STRING (or MAP/ARRAY for complex data)
  * ARRAY → ARRAY<STRING>
  * OBJECT → MAP<STRING, STRING>
- Handle nullability: "YES"/"NO" → NULL/NOT NULL
- Include column defaults where applicable
- Add table comments using COMMENT clause
- Generate proper CREATE TABLE syntax for Databricks

For each table metadata object, generate the equivalent Databricks CREATE TABLE statement.

Context: {context}
Table Metadata: {table_metadata}

Provide only the generated CREATE TABLE SQL statement, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create table translation system prompt."""
        return cls.system_prompt(**kwargs)
