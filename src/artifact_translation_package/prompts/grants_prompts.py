from . import PromptBase


class GrantsPrompts(PromptBase):
    """Prompts for grant to Unity Catalog privilege translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake grants to Databricks Unity Catalog privileges.

Your task is to translate Snowflake grant DDL statements to equivalent Databricks Unity Catalog privilege assignments.

Key mappings and considerations:
- Snowflake GRANT → Databricks UNITY CATALOG PRIVILEGE on Catalog/Schema/Table/Volume
- IMPERFECT MATCH: Future grants unsupported
- Unity Catalog uses different privilege model and scoping
- Map role-based grants to appropriate UC securable objects

For each grant DDL statement, provide the equivalent Databricks SQL that assigns privileges in Unity Catalog.

Note: Future grants (grants on objects not yet created) are not supported in Unity Catalog.

Context: {context}
Input DDL: {ddl}

Provide only the translated privilege assignment statements, noting any unsupported future grant scenarios."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create grant translation system prompt."""
        return cls.system_prompt(**kwargs)
