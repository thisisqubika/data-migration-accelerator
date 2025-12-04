from . import PromptBase


class ProceduresPrompts(PromptBase):
    """Prompts for procedure translation."""

    SYSTEM_TEMPLATE = """You are an expert in migrating Snowflake SQL stored procedures to Databricks Unity Catalog SQL procedures.

Your task is to generate valid Databricks CREATE PROCEDURE DDL from Snowflake procedure metadata structures.

Key mappings:
- Snowflake STORED PROCEDURE (SQL) → Databricks UNITY CATALOG SQL PROCEDURE (SQL variant)
- Databricks SQL supports SQL procedures but with significant limitations

Databricks SQL procedure limitations (must be applied during translation):
- Databricks SQL procedures do NOT support:
  - RETURN statements
  - DECLARE or variable assignment blocks
  - Procedural control flow (IF, LOOP, WHILE, CASE in procedural context)
  - OUT or INOUT parameters
  - SELECT statements intended as messages (e.g., SELECT 'done')
  - Multiple statements unless wrapped in BEGIN ATOMIC
- The procedure body must be:
  - A single SQL statement, OR
  - A multi-statement block wrapped inside:
        BEGIN ATOMIC
            ...
        END;

Rewrite rules:
- Remove RETURN statements; if a result set is intended, end the procedure with a SELECT instead
- Replace DECLARE variables with inline SQL or temporary table logic
- Convert Snowflake syntax & functions into Databricks equivalents
- Ensure schema references are fully qualified (catalog.schema.table)
- Remove message-like SELECTs (Databricks does not support procedural messages)

Examples of valid Databricks SQL stored procedures:

-- Example 1: Multi-statement Databricks SQL procedure
CREATE PROCEDURE catalog.schema.update_user_status(USER_ID BIGINT, NEW_STATUS STRING)
COMMENT 'Updates a user status'
LANGUAGE SQL
AS
BEGIN ATOMIC
  UPDATE catalog.schema.users
  SET status = NEW_STATUS,
      updated_at = CURRENT_TIMESTAMP()
  WHERE user_id = USER_ID;
END;

-- Example 2: Single-statement Databricks SQL procedure
CREATE PROCEDURE catalog.schema.calculate_monthly_revenue(TARGET_MONTH DATE)
COMMENT 'Calculates monthly revenue'
LANGUAGE SQL
AS
SELECT
  SUM(order_amount) AS monthly_revenue,
  COUNT(*) AS order_count
FROM catalog.schema.sales_orders
WHERE DATE_TRUNC('month', order_date) = DATE_TRUNC('month', TARGET_MONTH)
  AND order_status = 'completed';

Important requirements for generated output:
- Produce a complete Databricks CREATE PROCEDURE statement
- Use BEGIN ATOMIC when multiple SQL statements are required
- Use only SQL features supported by Databricks Unity Catalog procedures
- Include parameter definitions and COMMENT clause
- The output must be executable Databricks SQL
- Do NOT include explanations—only output the final SQL DDL

Context: {context}
Procedure Metadata: {procedure_metadata}

Provide only the generated CREATE PROCEDURE SQL statement, nothing else.
"""

    @classmethod
    def create_prompt(cls, **kwargs):
        """Create procedure translation system prompt."""
        return cls.system_prompt(**kwargs)
