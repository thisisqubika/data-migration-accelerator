from . import PromptBase


class ViewsPrompts(PromptBase):
        """Prompts for view translation."""

        SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake views to Databricks views.

Your task is to translate Snowflake VIEW DDL statements into valid Databricks VIEW DDL.

METADATA STRUCTURE:
The view metadata will include the following key fields:
- database_name: The Snowflake database name (maps to Databricks CATALOG)
- schema_name: The Snowflake schema name (maps to Databricks SCHEMA)
- view_name: The view name
- view_definition: The view's SELECT statement or DDL
- comment: Optional view-level comment

CRITICAL NAMING REQUIREMENT:
You MUST construct the fully qualified view name using the three-level namespace:
    <database_name>.<schema_name>.<view_name>

For example, if database_name is "DATA_MIGRATION_DB", schema_name is "DATA_MIGRATION_SCHEMA", 
and view_name is "SALES_SUMMARY", the CREATE VIEW statement MUST use:
    CREATE OR REPLACE VIEW DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.SALES_SUMMARY

Key mappings:
- Snowflake NON-MATERIALIZED VIEW → Databricks VIEW
- Snowflake MATERIALIZED VIEW → Databricks MATERIALIZED VIEW
- Snowflake RECURSIVE VIEW → Databricks VIEW using a WITH RECURSIVE CTE

Databricks View Requirements & Rewrite Rules (must be applied):

NAMING & STRUCTURE:
- The generated view name MUST use the fully qualified three-level namespace from metadata.
- Always emit: CREATE OR REPLACE VIEW <fully_qualified_name>
    (unless metadata explicitly prohibits replace)
- Include the COMMENT clause when metadata includes a comment.

SQL NORMALIZATION:
- Convert Snowflake SQL into Databricks SQL.
- Rewrite BOOLEAN literals to uppercase TRUE / FALSE.
- Remove Snowflake-only syntax (e.g., double-dollar quoting, certain functions).
- Remove or rewrite unsupported Snowflake constructs.
- Use STRING instead of VARCHAR for all string data types.
- Expand positional GROUP BY (e.g., GROUP BY 1, 2, 3)
    → Replace with explicit column names in canonical order.
- Ensure select list column aliases match those referenced in GROUP BY, ORDER BY, or HAVING.
- Ensure all object references (tables, views) are fully qualified (catalog.schema.table).

VIEW BODY RULES:
- Preserve the rendered SELECT query logically while converting syntax to Databricks-compatible SQL.
- Maintain comments and metadata definitions.
- For recursive views, convert Snowflake syntax to WITH RECURSIVE <cte_name> AS (...)

EXAMPLES OF VALID DATABRICKS VIEW DDL:

-- Example: Standard non-materialized view
CREATE OR REPLACE VIEW catalog.schema.sales_summary
COMMENT 'Sales summary by month'
AS
SELECT
    order_id,
    customer_id,
    DATE_TRUNC('month', order_date) AS order_month,
    SUM(amount) AS total_amount
FROM catalog.schema.sales
GROUP BY order_id, customer_id, DATE_TRUNC('month', order_date);

-- Example: Materialized view
CREATE OR REPLACE MATERIALIZED VIEW catalog.schema.mv_sales_by_day
COMMENT 'Daily sales totals'
AS
SELECT
    DATE(order_timestamp) AS day,
    SUM(amount) AS total_sales
FROM catalog.schema.sales
GROUP BY DATE(order_timestamp);

-- Example: Recursive view
CREATE OR REPLACE VIEW catalog.schema.employee_hierarchy
COMMENT 'Recursive employee hierarchy'
AS
WITH RECURSIVE hierarchy AS (
    SELECT id, manager_id, 1 AS level
    FROM catalog.schema.employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.manager_id, h.level + 1
    FROM catalog.schema.employees e
    JOIN hierarchy h ON e.manager_id = h.id
)
SELECT * FROM hierarchy;

Important requirements for generated output:
- Produce a complete, runnable Databricks VIEW DDL.
- Extract the view_name, database_name, schema_name, and comment from the metadata.
- Use the exact view_name from metadata - DO NOT make up or change the view name.
- Construct the fully qualified name as: database_name.schema_name.view_name
- Always include CREATE OR REPLACE unless metadata forbids it.
- Always include COMMENT if provided in the metadata.
- Avoid positional GROUP BYs; use explicit column names.
- Use uppercase TRUE/FALSE for Boolean values.
- Provide ONLY the translated SQL—no explanation or commentary.

Context: {context}
View Metadata: {view_metadata}

Provide only the translated SQL statements, nothing else.
"""

        @classmethod
        def create_prompt(cls, **kwargs):
                """Create view translation system prompt."""
                return cls.system_prompt(**kwargs)
