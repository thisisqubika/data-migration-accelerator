from . import PromptBase


class EvaluationPrompts(PromptBase):
    """Prompts for SQL evaluation node."""

    SYSTEM_TEMPLATE = """You are an expert in Databricks SQL syntax validation.

Your task is to evaluate SQL DDL statements to check if they have valid Databricks SQL syntax.

Databricks SQL Syntax Requirements:
- Valid CREATE statements (CREATE TABLE, CREATE VIEW, CREATE SCHEMA, CREATE CATALOG, CREATE PROCEDURE, etc.)
- Proper data types (STRING, INT, BIGINT, DECIMAL, BOOLEAN, TIMESTAMP, DATE, etc.)
- Valid clauses (PARTITIONED BY, CLUSTER BY, TBLPROPERTIES, etc.)
- Proper identifier quoting and naming conventions
- Valid catalog.schema.table references
- Valid procedure syntax with LANGUAGE SQL and proper AS/BEGIN/END blocks
- Valid SQL statements within procedures
SQL Statements to Evaluate:
{sql_content}

Evaluate each SQL statement and provide a structured assessment. You must return a JSON object with the following structure:
{{
    "results": [
        {{
            "syntax_valid": boolean,
            "error_message": string (null if syntax is valid),
            "issues": [
                {{
                    "description": string,
                    "line_number": integer (optional),
                    "suggestion": string (optional)
                }}
            ]
        }}
    ]
}}

The results array must contain one evaluation result for each SQL statement provided, in the same order.
Focus only on syntax validity. If syntax is valid, set syntax_valid to true and error_message to null. If syntax is invalid, set syntax_valid to false and provide a clear error_message describing the syntax error."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create evaluation system prompt."""
        return cls.system_prompt(**kwargs)
