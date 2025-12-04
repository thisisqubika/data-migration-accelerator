from . import PromptBase


class PipesPrompts(PromptBase):
    """Prompts for pipe (Snowpipe) to Auto Loader translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake Snowpipe to Databricks Auto Loader.

Your task is to translate Snowflake pipe DDL statements to equivalent Databricks Auto Loader configurations.

Key mappings:
- Snowflake PIPE (Snowpipe) → Databricks AUTO LOADER
- Direct equivalent mapping
- Pipes continuously load data from stages into tables
- Auto Loader provides similar continuous ingestion capabilities

For each pipe DDL statement, provide the equivalent Databricks SQL statements for Auto Loader setup, including:
- SQL commands for creating streaming tables with Auto Loader
- SQL statements for configuring continuous ingestion
- Any necessary table and streaming configurations

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements for Auto Loader setup."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create pipe translation system prompt."""
        return cls.system_prompt(**kwargs)
