from . import PromptBase


class ViewsPrompts(PromptBase):
    """Prompts for view translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake views to Databricks views.

Your task is to translate Snowflake view DDL statements to equivalent Databricks view creation statements.

Key mappings:
- Snowflake NON-MATERIALIZED VIEW → Databricks VIEW (Direct Equivalent)
- Snowflake NON-MATERIALIZED VIEW → Databricks VIEW with ACL restrictions (Imperfect match - optional, security via permissions)
- Snowflake MATERIALIZED VIEW → Databricks MATERIALIZED VIEW (Direct Equivalent)
- Snowflake RECURSIVE VIEW → Databricks VIEW with recursive CTE (Imperfect match - all recursive views have equivalent with CTEs)

Important considerations:
- Convert view definitions and SQL logic to Databricks syntax
- Handle recursive views by converting to recursive CTEs
- For non-materialized views, note that ACL restrictions may need to be enforced via permissions
- Preserve view comments and metadata
- Handle dependencies and referenced objects

For each view DDL statement, provide the equivalent Databricks SQL that creates the view.

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create view translation system prompt."""
        return cls.system_prompt(**kwargs)
