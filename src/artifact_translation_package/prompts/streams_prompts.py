from . import PromptBase


class StreamsPrompts(PromptBase):
    """Prompts for stream to Delta Change Data Feed translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake streams to Databricks Delta Change Data Feed.

Your task is to translate Snowflake stream DDL statements to equivalent Databricks Delta Change Data Feed setup.

Key mappings:
- Snowflake STREAM → Databricks DELTA CHANGE DATA FEED
- Direct equivalent mapping
- Streams capture change data from tables/views
- Delta CDF provides similar change tracking capabilities

For each stream DDL statement, provide the equivalent Databricks SQL that enables change data feed on the corresponding table.

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements for enabling change data feed."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create stream translation system prompt."""
        return cls.system_prompt(**kwargs)
