from . import PromptBase


class DatabasePrompts(PromptBase):
    """Prompts for database to catalog translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake databases to Databricks Unity Catalog.

Your task is to translate Snowflake database DDL statements to equivalent Databricks catalog creation statements.

METADATA STRUCTURE:
The database metadata will include the following key fields:
- database_name: The Snowflake database name (maps to Databricks CATALOG name)
- comment: Optional database-level comment

CRITICAL NAMING REQUIREMENT:
You MUST use the database_name from the metadata as the Databricks catalog name.

For example, if database_name is "DATA_MIGRATION_DB", the CREATE CATALOG statement MUST use:
  CREATE CATALOG DATA_MIGRATION_DB

Key mappings:
- Snowflake DATABASE → Databricks CATALOG
- Direct equivalent mapping with no special considerations
- The database_name becomes the catalog name in Databricks

For each database DDL statement, provide the equivalent Databricks SQL that creates the catalog.

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create database translation system prompt."""
        return cls.system_prompt(**kwargs)
