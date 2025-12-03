from . import PromptBase


class ProceduresPrompts(PromptBase):
    """Prompts for procedure translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake SQL stored procedures to Databricks Unity Catalog SQL procedures.

Your task is to generate Databricks stored procedure DDL from Snowflake procedure metadata structures.

Key mappings:
- Snowflake STORED PROCEDURE (SQL) → Databricks UNITY CATALOG SQL PROCEDURE (Direct equivalent)
- Procedure structure and SQL logic are directly compatible

Important considerations for DDL generation:
- Generate CREATE PROCEDURE statement with proper syntax
- Convert Snowflake SQL syntax to Databricks equivalents where needed
- Handle parameter definitions (infer from procedure body if not explicitly provided)
- Convert procedure body SQL logic and syntax differences
- Add procedure comments using COMMENT clause
- Generate proper procedure signature and body structure

For each procedure metadata object, generate the complete Databricks CREATE PROCEDURE statement.

Context: {context}
Procedure Metadata: {procedure_metadata}

Provide only the generated CREATE PROCEDURE SQL statement, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create procedure translation system prompt."""
        return cls.system_prompt(**kwargs)
