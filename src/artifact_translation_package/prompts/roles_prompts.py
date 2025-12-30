from . import PromptBase


class RolesPrompts(PromptBase):
    """Prompts for role translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake roles to Databricks Unity Catalog groups.

Your task is to translate Snowflake role metadata (provided as JSON in {metadata}) into equivalent Databricks SQL
statements that create Unity Catalog groups, and (only if explicit relationships are provided) group membership
statements that represent role inheritance.

INPUT (METADATA) STRUCTURE:
The metadata will be a JSON object that may include:
- database: string (context only; groups are account-level)
- schema: string (context only; groups are account-level)
- roles: array of role objects, each may include:
  - name: string (required)                  # Snowflake role name
  - comment: string (optional)               # Role description (may be empty)
  - owner: string (optional)                 # Informational only in Databricks
  - is_inherited: "Y" | "N" (optional)       # Informational only
  - assigned_to_users: number (optional)     # Informational only
  - granted_roles: number (optional)         # Informational only (counts, not edges)
  - granted_to_roles: number (optional)      # Informational only (counts, not edges)

IMPORTANT: The metadata may also include explicit role hierarchy information in one of these forms:
- parent_role on a role object, e.g. {{ "name": "CHILD", "parent_role": "PARENT" }}
- a grants/edges list, e.g. metadata.role_grants: [{{ "role": "CHILD", "granted_to_role": "PARENT" }}, ...]
If explicit relationships are NOT present (only counts are present), DO NOT guess membership.

CRITICAL NAMING REQUIREMENT:
Snowflake roles map 1:1 to Databricks Unity Catalog groups.
Groups are ACCOUNT-LEVEL objects and MUST NOT be qualified with catalog/schema/database.
Use the role "name" exactly as the group name, wrapped in backticks.

Always generate rerunnable SQL using:
  CREATE GROUP IF NOT EXISTS `ROLE_NAME`;

COMMENTS / DESCRIPTIONS:
- If comment is a non-empty string, preserve it as a single-line SQL comment immediately above the CREATE GROUP.
- If comment is empty or missing, do not invent one.

OWNERSHIP:
- Do NOT emit ownership SQL. Treat owner as informational only.

ROLE HIERARCHY / INHERITANCE (ONLY WHEN EXPLICIT):
Represent inheritance using group membership statements:
  ALTER GROUP `PARENT` ADD MEMBER `CHILD`;
Meaning: CHILD becomes a member of PARENT (PARENT inherits CHILD permissions).

OUTPUT REQUIREMENTS:
- Output ONLY Databricks SQL, no explanations.
- Emit CREATE GROUP for every role in metadata.roles[].
- Sort roles by name ascending for deterministic output.
- Emit membership statements AFTER all CREATE GROUP statements.
- Sort membership statements deterministically (by parent then child).
- Do not generate statements for relationships that are not explicitly provided.

Context: {context}
Metadata: {metadata}

Provide only the translated SQL statements for group creation and (if explicitly available) membership hierarchy."""
    
    @classmethod
    def create_prompt(cls, **kwargs):
        """Create role translation system prompt."""
        return cls.system_prompt(**kwargs)
