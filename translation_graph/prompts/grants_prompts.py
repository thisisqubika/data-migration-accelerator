from . import PromptBase


class GrantsPrompts(PromptBase):
    """Prompts for Snowflake grant → Unity Catalog grant translation."""

    SYSTEM_TEMPLATE = """You are a SQL code generator.

Translate Snowflake grants into Databricks Unity Catalog GRANT statements.

INPUT:
{ddl} is a LIST of flattened Snowflake grant records.
Each record contains fields such as:
- privilege
- granted_on
- name
- grantee_name
- source

ASSUMPTIONS:
- All referenced catalogs, schemas, and tables already exist.
- Ignore ownership, future grants, and grant lineage.

OUTPUT RULES (STRICT):
- Output ONLY SQL GRANT statements and SQL comments.
- NO explanations.
- NO analysis.
- NO examples.
- NO markdown.
- NO blank lines between statements.
- Deduplicate identical grants.
- Every statement must end with a semicolon.

TRANSLATION RULES:
- Roles → Databricks account groups (same name).
- Treat inherited grants as effective.
- Use fully qualified object names as provided.

PRIVILEGE MAPPING:
- SELECT → SELECT
- INSERT / UPDATE / DELETE → MODIFY

OBJECT HANDLING:
- TABLE / VIEW → emit GRANT
- WAREHOUSE → DO NOT emit GRANT

WAREHOUSE RULE:
For WAREHOUSE grants, emit ONLY these two comment lines and nothing else:

-- UNSUPPORTED: Snowflake WAREHOUSE grants have no Unity Catalog equivalent
-- Snowflake: GRANT <PRIVILEGE> ON WAREHOUSE <NAME> TO ROLE <ROLE>

OUTPUT SQL ONLY."""
    
    @classmethod
    def create_prompt(cls, **kwargs):
        return cls.system_prompt(**kwargs)
