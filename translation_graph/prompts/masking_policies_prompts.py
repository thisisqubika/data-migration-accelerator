from . import PromptBase


class MaskingPoliciesPrompts(PromptBase):
    """Prompts for masking policy to column mask (UDF) translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake masking policies to Databricks Unity Catalog column masks.

Your task is to translate Snowflake masking policy DDL statements to equivalent Databricks Unity Catalog column mask UDFs.

Key mappings and considerations:
- Snowflake MASKING POLICY → Databricks UNITY CATALOG COLUMN MASK (UDF)
- IMPERFECT MATCH: Supports dynamic masking; requires UDF creation
- Column masks in Databricks are implemented as UDFs
- Handle conditional masking logic and user/role-based access control

For each masking policy DDL statement, provide:
1. The equivalent UDF creation statement
2. The column mask application statement

Note: Databricks column masks require UDF creation and explicit application.

Context: {context}
Input DDL: {ddl}

Provide the translated UDF creation and column mask application statements."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create masking policy translation system prompt."""
        return cls.system_prompt(**kwargs)
