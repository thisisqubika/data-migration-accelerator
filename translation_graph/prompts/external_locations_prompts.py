from . import PromptBase


class ExternalLocationsPrompts(PromptBase):
    """Prompts for external location translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake external locations to Databricks external locations.

Your task is to generate Databricks external location DDL from Snowflake external location metadata structures.

METADATA STRUCTURE:
The external location metadata may include the following key fields:
- location_name: The external location name
- url: The storage URL/path
- storage_integration: The storage credential reference
- comment: Optional location-level comment

Note: External locations in Databricks are account-level objects and don't have a database/schema hierarchy.

CRITICAL NAMING REQUIREMENT:
External locations are named objects without catalog/schema qualification. Use the location_name directly.

For example, if location_name is "S3_DATA_LOCATION", the CREATE EXTERNAL LOCATION statement uses:
  CREATE EXTERNAL LOCATION S3_DATA_LOCATION

Key mappings:
- Snowflake EXTERNAL LOCATION → Databricks EXTERNAL LOCATION (Direct Equivalent)
- Storage integrations map to storage credentials in Databricks
- External locations define storage paths that can be referenced by external tables, stages, and volumes

Important considerations for DDL generation:
- Convert URL paths to Databricks external location syntax
- Map storage integrations to storage credentials using WITH (STORAGE CREDENTIAL ...)
- Handle different cloud storage types (S3, Azure, GCS)
- Include location comments using COMMENT clause
- Generate proper CREATE EXTERNAL LOCATION syntax for Databricks Unity Catalog
- External locations are account-level objects without catalog/schema prefix

For each external location metadata object, generate the equivalent Databricks CREATE EXTERNAL LOCATION statement.

Context: {context}
External Location Metadata: {external_location_metadata}

Provide only the generated CREATE EXTERNAL LOCATION SQL statement, no explanations."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create external location translation system prompt."""
        return cls.system_prompt(**kwargs)






