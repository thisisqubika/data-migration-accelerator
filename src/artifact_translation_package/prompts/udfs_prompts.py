from . import PromptBase


class UDFsPrompts(PromptBase):
    """Prompts for UDF translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake SQL UDFs to Databricks Unity Catalog SQL UDFs.

Your task is to generate Databricks UDF DDL from Snowflake function metadata structures.

Key mappings:
- Snowflake UDFs (SQL) → Databricks UNITY CATALOG SQL UDF
- Direct equivalent mapping
- Handle function signatures, return types, and SQL logic
- Consider any syntax differences between platforms

Important considerations for DDL generation:
- Generate CREATE FUNCTION statement with proper syntax
- Convert Snowflake SQL syntax to Databricks equivalents
- Infer function parameters and return types from the SQL body
- Handle function body SQL logic and syntax differences
- Add function comments using COMMENT clause
- Generate proper function signature and body structure

For each function metadata object, generate the complete Databricks CREATE FUNCTION statement.

Context: {context}
Function Metadata: {function_metadata}

Provide only the generated CREATE FUNCTION SQL statement, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create UDF translation system prompt."""
        return cls.system_prompt(**kwargs)
