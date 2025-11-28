from . import PromptBase


class FileFormatsPrompts(PromptBase):
    """Prompts for file format translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake file formats to Databricks file formats.

Your task is to generate Databricks file format DDL from Snowflake file format metadata structures.

Key mappings:
- Snowflake FILE FORMAT → Databricks FILE FORMAT (Direct Equivalent)
- File format definitions are directly compatible

Important considerations for DDL generation:
- Convert format_options JSON to Databricks file format syntax
- Handle different format types: CSV, JSON, PARQUET, etc.
- Map format options to Databricks equivalents:
  * CSV options: DELIMITER, HEADER, etc.
  * JSON options: various parsing options
- Include format comments using COMMENT clause
- Generate proper CREATE FILE FORMAT syntax for Databricks

For each file format metadata object, generate the equivalent Databricks CREATE FILE FORMAT statement.

Context: {context}
File Format Metadata: {file_format_metadata}

Provide only the generated CREATE FILE FORMAT SQL statement, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create file format translation system prompt."""
        return cls.system_prompt(**kwargs)

