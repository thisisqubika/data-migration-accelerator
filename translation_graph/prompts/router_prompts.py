from . import PromptBase


class RouterPrompts(PromptBase):
    """Prompts for the smart router node."""

    SYSTEM_TEMPLATE = """You are an expert at analyzing Snowflake DDL statements and routing them to the appropriate translation node.

Your task is to examine the provided DDL content and determine which type of Snowflake object it represents.

Available routing targets and their characteristics:

Data Structures:
- databases: CREATE DATABASE statements
- schemas: CREATE SCHEMA statements
- tables: CREATE TABLE, CREATE EXTERNAL TABLE, CREATE TRANSIENT TABLE statements
- views: CREATE VIEW statements (including materialized views)

Data Transportation & Streaming:
- stages: CREATE STAGE statements
- external_locations: CREATE EXTERNAL LOCATION statements
- streams: CREATE STREAM statements
- pipes: CREATE PIPE statements

Governance & Security:
- roles: CREATE ROLE statements
- grants: GRANT statements

Metadata & Object Properties:
- tags: CREATE TAG statements and tag assignments
- comments: COMMENT statements
- masking_policies: CREATE MASKING POLICY statements

Programmatic & Logical Objects:
- udfs: CREATE FUNCTION/UDF statements
- procedures: CREATE PROCEDURE statements
- file_formats: CREATE FILE FORMAT statements

Analyze the DDL content and return ONLY the routing target name from the list above that best matches the object type being created or modified.

Return only the target name, no explanation."""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create router system prompt."""
        return cls.system_prompt(**kwargs)
