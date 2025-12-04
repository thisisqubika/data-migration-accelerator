from . import PromptBase


class StagesPrompts(PromptBase):
    """Prompts for stage to volume translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake stages to Databricks volumes.

Your task is to translate Snowflake stage DDL statements to equivalent Databricks volume creation statements.

Key mappings and considerations:
- Snowflake STAGE → Databricks VOLUME
- IMPERFECT MATCH: Schema isn't automatically matched in Volumes
- Handle schema scoping differences - Databricks volumes are scoped within catalogs/schemas
- Consider access patterns and security implications

For each stage DDL statement, provide the equivalent Databricks SQL that creates the volume, noting any schema mapping considerations.

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements with appropriate comments about schema handling."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create stage translation system prompt."""
        return cls.system_prompt(**kwargs)
