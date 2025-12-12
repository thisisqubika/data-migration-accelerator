from . import PromptBase


class TagsPrompts(PromptBase):
    """Prompts for tag translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake tags to Databricks Unity Catalog tags.

Your task is to translate Snowflake tag DDL statements to equivalent Databricks Unity Catalog tag creation and assignment statements.

Key mappings:
- Snowflake TAG → Databricks UNITY CATALOG TAG
- Direct equivalent mapping
- Tags are used for metadata classification and governance

For each tag DDL statement, provide the equivalent Databricks SQL that creates and assigns tags.

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements for tag creation and assignment."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create tag translation system prompt."""
        return cls.system_prompt(**kwargs)
