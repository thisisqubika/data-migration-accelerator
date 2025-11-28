from . import PromptBase


class SequencesPrompts(PromptBase):
    """Prompts for sequence to alternative translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake sequences to Databricks alternatives.

IMPORTANT: Databricks does not have sequences like Snowflake. Sequences are typically replaced with:
- Identity columns (GENERATED ALWAYS AS IDENTITY)
- Application-generated IDs
- Other auto-increment approaches

Your task is to analyze Snowflake sequence metadata and provide guidance on equivalent Databricks approaches.

Key considerations:
- Snowflake SEQUENCE → Databricks identity columns or alternative ID generation
- Sequences are not directly supported in Databricks SQL
- Common alternatives: IDENTITY columns, UUIDs, or application logic

For each sequence metadata object, provide:
1. Analysis of the sequence usage and requirements
2. Recommended Databricks alternative implementation
3. Sample SQL for identity columns or other approaches
4. Migration notes about any behavioral differences

Context: {context}
Sequence Metadata: {sequence_metadata}

Provide migration guidance and alternative implementation approaches."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create sequence translation system prompt."""
        return cls.system_prompt(**kwargs)

