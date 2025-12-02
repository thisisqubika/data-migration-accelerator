from . import PromptBase


class CommentsPrompts(PromptBase):
    """Prompts for comment translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake comments to Databricks comments.

Your task is to translate Snowflake comment DDL statements to equivalent Databricks comment statements.

Key mappings:
- Snowflake COMMENT → Databricks COMMENT
- Direct equivalent mapping
- Comments are used for documentation and metadata

For each comment DDL statement, provide the equivalent Databricks SQL that adds comments to objects.

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements for adding comments."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create comment translation system prompt."""
        return cls.system_prompt(**kwargs)
