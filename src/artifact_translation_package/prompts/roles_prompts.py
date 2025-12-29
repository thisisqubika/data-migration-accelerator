from . import PromptBase


class RolesPrompts(PromptBase):
    """Prompts for role translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake roles to Databricks Unity Catalog groups.

Your task is to translate Snowflake role DDL statements to equivalent Databricks Unity Catalog group creation statements.

METADATA STRUCTURE:
The role metadata may include the following key fields:
- role_name: The Snowflake role name
- comment: Optional role-level comment
- parent_role: Optional parent role for role hierarchy

Note: Roles in Databricks Unity Catalog are account-level objects (groups) and don't have a database/schema hierarchy.

CRITICAL NAMING REQUIREMENT:
Roles map to groups which are account-level objects without catalog/schema qualification. Use the role_name directly as the group name.

For example, if role_name is "ANALYST_ROLE", the CREATE GROUP statement uses:
  CREATE GROUP `ANALYST_ROLE`

Key mappings:
- Snowflake ROLE → Databricks UNITY CATALOG GROUP (Direct Equivalent)
- Role inheritance becomes group membership hierarchy
- Roles map directly to Groups in Unity Catalog

Important considerations:
- Convert role creation statements to group creation
- Handle role hierarchies by mapping to group membership relationships
- Preserve role comments and descriptions
- Map role ownership appropriately
- Groups are account-level objects without catalog/schema prefix

For each role DDL statement, provide the equivalent Databricks SQL that creates the Unity Catalog group.

Context: {context}
Metadata: {metadata}

Provide only the translated SQL statements for group creation and membership hierarchy."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create role translation system prompt."""
        return cls.system_prompt(**kwargs)
