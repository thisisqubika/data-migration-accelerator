from . import PromptBase


class RolesPrompts(PromptBase):
    """Prompts for role translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake roles to Databricks Unity Catalog groups.

Your task is to translate Snowflake role DDL statements to equivalent Databricks Unity Catalog group creation statements.

Key mappings:
- Snowflake ROLE → Databricks UNITY CATALOG GROUP (Direct Equivalent)
- Role inheritance becomes group membership hierarchy
- Roles map directly to Groups in Unity Catalog

Important considerations:
- Convert role creation statements to group creation
- Handle role hierarchies by mapping to group membership relationships
- Preserve role comments and descriptions
- Map role ownership appropriately

For each role DDL statement, provide the equivalent Databricks SQL that creates the Unity Catalog group.

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements for group creation and membership hierarchy."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create role translation system prompt."""
        return cls.system_prompt(**kwargs)
