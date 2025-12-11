from . import PromptBase


class StagesPrompts(PromptBase):
    """Prompts for stage to volume translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake stages to Databricks volumes.

Your task is to translate Snowflake stage DDL statements to equivalent Databricks volume creation statements.

METADATA STRUCTURE:
The stage metadata will include the following key fields:
- database_name: The Snowflake database name (maps to Databricks CATALOG)
- schema_name: The Snowflake schema name (maps to Databricks SCHEMA)
- stage_name: The stage name
- stage_location: The storage location/URL
- comment: Optional stage-level comment

CRITICAL NAMING REQUIREMENT:
You MUST construct the fully qualified volume name using the three-level namespace:
  <database_name>.<schema_name>.<stage_name>

For example, if database_name is "DATA_MIGRATION_DB", schema_name is "DATA_MIGRATION_SCHEMA", 
and stage_name is "MY_STAGE", the CREATE VOLUME statement MUST use:
  CREATE VOLUME DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.MY_STAGE

Key mappings and considerations:
- Snowflake STAGE → Databricks VOLUME
- IMPERFECT MATCH: Schema isn't automatically matched in Volumes
- Handle schema scoping differences - Databricks volumes are scoped within catalogs/schemas
- Consider access patterns and security implications
- Always use the fully qualified three-level namespace from the metadata

For each stage DDL statement, provide the equivalent Databricks SQL that creates the volume, noting any schema mapping considerations.

Context: {context}
Input DDL: {ddl}

Provide only the translated SQL statements with appropriate comments about schema handling."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create stage translation system prompt."""
        return cls.system_prompt(**kwargs)
