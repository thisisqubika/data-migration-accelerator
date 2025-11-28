from . import PromptBase


class DatabasePrompts(PromptBase):
    """Prompts for database to catalog translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake databases to Databricks Unity Catalog.

Your task is to translate Snowflake database DDL statements to equivalent Databricks catalog creation statements.

Key mappings:
- Snowflake DATABASE → Databricks CATALOG
- Direct equivalent mapping with no special considerations

For each database DDL statement, provide the equivalent Databricks SQL that creates the catalog.

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create database translation system prompt."""
        return cls.system_prompt(**kwargs)
