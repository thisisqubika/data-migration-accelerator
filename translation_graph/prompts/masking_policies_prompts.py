from . import PromptBase


class MaskingPoliciesPrompts(PromptBase):
    """Prompts for masking policy to column mask (UDF) translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake masking policies to Databricks Unity Catalog column masks.

Your task is to translate Snowflake masking policy DDL statements to equivalent Databricks Unity Catalog column mask UDFs.

METADATA STRUCTURE:
The masking policy metadata may include the following key fields:
- database_name: The Snowflake database name (maps to Databricks CATALOG)
- schema_name: The Snowflake schema name (maps to Databricks SCHEMA)
- policy_name: The masking policy name
- policy_definition: The masking logic/expression
- comment: Optional policy-level comment

CRITICAL NAMING REQUIREMENT:
When creating masking UDFs and applying them, use fully qualified names:
- For the masking UDF: <database_name>.<schema_name>.<policy_name>_mask
- When applying to tables: <database_name>.<schema_name>.<table_name>

For example, creating a masking policy "SSN_MASK" in database "DATA_MIGRATION_DB", schema "DATA_MIGRATION_SCHEMA":
  CREATE FUNCTION DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.SSN_MASK_mask(...)

Key mappings and considerations:
- Snowflake MASKING POLICY → Databricks UNITY CATALOG COLUMN MASK (UDF)
- IMPERFECT MATCH: Supports dynamic masking; requires UDF creation
- Column masks in Databricks are implemented as UDFs
- Handle conditional masking logic and user/role-based access control
- All object references must be fully qualified

For each masking policy DDL statement, provide:
1. The equivalent UDF creation statement (fully qualified)
2. The column mask application statement (with fully qualified table names)

Note: Databricks column masks require UDF creation and explicit application.

Context: {context}
Input DDL: {ddl}

Provide the translated UDF creation and column mask application statements."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create masking policy translation system prompt."""
        return cls.system_prompt(**kwargs)
