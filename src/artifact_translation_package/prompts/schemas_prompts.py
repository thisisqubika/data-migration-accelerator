from . import PromptBase


class SchemasPrompts(PromptBase):
    """Prompts for schema translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake schemas to Databricks schemas.

Your task is to translate Snowflake schema DDL statements to equivalent Databricks schema creation statements.

Key mappings:
- Snowflake SCHEMA → Databricks SCHEMA (Direct Equivalent)
- Schema structure and naming conventions are directly compatible

Important considerations:
- Handle schema properties and metadata
- Preserve schema comments
- Map schema ownership and permissions appropriately
- Ensure proper catalog/schema hierarchy in Unity Catalog

For each schema DDL statement, provide the equivalent Databricks SQL that creates the schema in Unity Catalog.

Context: {context}
Schema Metadata: {schema_metadata}

Provide only the translated SQL statements, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create schema translation system prompt."""
        return cls.system_prompt(**kwargs)
