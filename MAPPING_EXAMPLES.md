# Snowflake to Databricks Migration Mapping Examples

This document provides concrete examples of Snowflake objects and their corresponding Databricks equivalents, based on actual object structures extracted from Snowflake.

## Table of Contents

1. [Data Structures](#data-structures)
2. [Data Transportation & Streaming](#data-transportation--streaming)
3. [Governance & Security](#governance--security)
4. [Metadata & Object Properties](#metadata--object-properties)
5. [Programmatic & Logical Objects](#programmatic--logical-objects)

---

## Data Structures

### Database → Catalog

**Snowflake Input:**
```json
{
  "database": "DATA_MIGRATION_DB"
}
```

**Databricks Output:**
```sql
CREATE CATALOG IF NOT EXISTS data_migration_db
COMMENT 'Migrated from Snowflake database DATA_MIGRATION_DB';
```

**Mapping Rule:** Direct equivalent - Database maps directly to Catalog in Unity Catalog.

---

### Schema → Schema

**Snowflake Input:**
```json
{
  "schema": "DATA_MIGRATION_SCHEMA",
  "database": "DATA_MIGRATION_DB"
}
```

**Databricks Output:**
```sql
CREATE SCHEMA IF NOT EXISTS data_migration_db.data_migration_schema
COMMENT 'Migrated from Snowflake schema DATA_MIGRATION_SCHEMA';
```

**Mapping Rule:** Direct equivalent - Schema structure is directly compatible.

---

### Tables (Permanent) → Managed Tables

**Snowflake Input:**
```json
{
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "table_name": "DATA_MIGRATION_SOURCE",
  "table_type": "BASE TABLE",
  "row_count": 0,
  "bytes": 0,
  "created": "2025-11-20 12:53:20.787000-08:00",
  "last_altered": "2025-11-20 13:05:49.773000-08:00",
  "comment": "Data migration source table with various data types",
  "columns": [
    {
      "column_name": "SOURCE_ID",
      "data_type": "NUMBER",
      "character_maximum_length": null,
      "numeric_precision": 38,
      "numeric_scale": 0,
      "is_nullable": "NO",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "SOURCE_NAME",
      "data_type": "TEXT",
      "character_maximum_length": 100,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "SOURCE_TYPE",
      "data_type": "TEXT",
      "character_maximum_length": 100,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "SOURCE_CONNECTION",
      "data_type": "TEXT",
      "character_maximum_length": 255,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "SOURCE_STATUS",
      "data_type": "TEXT",
      "character_maximum_length": 50,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "MIGRATION_TIMESTAMP",
      "data_type": "TIMESTAMP_NTZ",
      "character_maximum_length": null,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "IS_ACTIVE",
      "data_type": "BOOLEAN",
      "character_maximum_length": null,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "RECORD_COUNT",
      "data_type": "NUMBER",
      "character_maximum_length": null,
      "numeric_precision": 18,
      "numeric_scale": 0,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "METADATA",
      "data_type": "VARIANT",
      "character_maximum_length": null,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "TAGS",
      "data_type": "ARRAY",
      "character_maximum_length": null,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "CONFIG",
      "data_type": "OBJECT",
      "character_maximum_length": null,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "CREATED_AT",
      "data_type": "TIMESTAMP_LTZ",
      "character_maximum_length": null,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": "CURRENT_TIMESTAMP()",
      "comment": null
    },
    {
      "column_name": "UPDATED_AT",
      "data_type": "TIMESTAMP_LTZ",
      "character_maximum_length": null,
      "numeric_precision": null,
      "numeric_scale": null,
      "is_nullable": "YES",
      "column_default": "CURRENT_TIMESTAMP()",
      "comment": null
    }
  ],
  "sample_data": []
}
```

**Databricks Output:**
```sql
CREATE TABLE IF NOT EXISTS data_migration_db.data_migration_schema.data_migration_source (
    source_id BIGINT NOT NULL,
    source_name VARCHAR(100),
    source_type VARCHAR(100),
    source_connection VARCHAR(255),
    source_status VARCHAR(50),
    migration_timestamp TIMESTAMP,
    is_active BOOLEAN,
    record_count BIGINT,
    metadata STRING,
    tags ARRAY<STRING>,
    config MAP<STRING, STRING>,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT 'Data migration source table with various data types';
```

---

**Snowflake Input (Second Table Example):**
```json
{
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "table_name": "DATA_MIGRATION_TARGET",
  "table_type": "BASE TABLE",
  "row_count": 0,
  "bytes": 0,
  "created": "2025-11-20 12:53:55.414000-08:00",
  "last_altered": "2025-11-20 12:53:55.569000-08:00",
  "comment": "Data migration target table with clustering and time travel",
  "columns": [
    {
      "column_name": "TARGET_ID",
      "data_type": "NUMBER",
      "numeric_precision": 38,
      "numeric_scale": 0,
      "is_nullable": "NO",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "SOURCE_ID",
      "data_type": "NUMBER",
      "numeric_precision": 38,
      "numeric_scale": 0,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "TARGET_NAME",
      "data_type": "TEXT",
      "character_maximum_length": 200,
      "is_nullable": "NO",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "TARGET_TYPE",
      "data_type": "TEXT",
      "character_maximum_length": 100,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "MIGRATION_DATE",
      "data_type": "DATE",
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "MIGRATION_STATUS",
      "data_type": "TEXT",
      "character_maximum_length": 50,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "TOTAL_RECORDS",
      "data_type": "NUMBER",
      "numeric_precision": 18,
      "numeric_scale": 0,
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "SUCCESS_COUNT",
      "data_type": "NUMBER",
      "numeric_precision": 10,
      "numeric_scale": 0,
      "is_nullable": "YES",
      "column_default": "0",
      "comment": null
    },
    {
      "column_name": "ERROR_COUNT",
      "data_type": "NUMBER",
      "numeric_precision": 10,
      "numeric_scale": 0,
      "is_nullable": "YES",
      "column_default": "0",
      "comment": null
    },
    {
      "column_name": "IS_ACTIVE",
      "data_type": "BOOLEAN",
      "is_nullable": "YES",
      "column_default": "TRUE",
      "comment": null
    },
    {
      "column_name": "MIGRATION_CONFIG",
      "data_type": "VARIANT",
      "is_nullable": "YES",
      "column_default": null,
      "comment": null
    },
    {
      "column_name": "CREATED_AT",
      "data_type": "TIMESTAMP_LTZ",
      "is_nullable": "YES",
      "column_default": "CURRENT_TIMESTAMP()",
      "comment": null
    },
    {
      "column_name": "UPDATED_AT",
      "data_type": "TIMESTAMP_LTZ",
      "is_nullable": "YES",
      "column_default": "CURRENT_TIMESTAMP()",
      "comment": null
    }
  ],
  "sample_data": []
}
```

**Databricks Output:**
```sql
CREATE TABLE IF NOT EXISTS data_migration_db.data_migration_schema.data_migration_target (
    target_id BIGINT NOT NULL,
    source_id BIGINT,
    target_name VARCHAR(200) NOT NULL,
    target_type VARCHAR(100),
    migration_date DATE,
    migration_status VARCHAR(50),
    total_records BIGINT,
    success_count INT DEFAULT 0,
    error_count INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    migration_config STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT 'Data migration target table with clustering and time travel';
```

**Data Type Mappings:**
- `NUMBER(38,0)` → `BIGINT`
- `NUMBER(18,0)` → `BIGINT`
- `NUMBER(10,0)` → `INT`
- `NUMBER(precision, scale)` → `DECIMAL(precision, scale)`
- `TEXT(length)` → `VARCHAR(length)`
- `TIMESTAMP_NTZ` → `TIMESTAMP`
- `TIMESTAMP_LTZ` → `TIMESTAMP`
- `DATE` → `DATE`
- `BOOLEAN` → `BOOLEAN`
- `VARIANT` → `STRING`
- `ARRAY` → `ARRAY<STRING>`
- `OBJECT` → `MAP<STRING, STRING>`

**Mapping Rule:** Direct equivalent - Permanent tables (BASE TABLE) map to managed tables.

---

### Views (Non-materialized) → Views

**Snowflake Input:**
```json
{
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "view_name": "DATA_MIGRATION_MONTHLY_SUMMARY",
  "view_definition": "CREATE OR REPLACE VIEW data_migration_monthly_summary AS\nSELECT \n    DATE_TRUNC('MONTH', migration_date) AS month,\n    COUNT(DISTINCT target_id) AS target_count,\n    COUNT(DISTINCT source_id) AS source_count,\n    SUM(total_records) AS total_records_migrated,\n    AVG(total_records) AS avg_records_per_target\nFROM data_migration_target\nWHERE migration_status = 'COMPLETED'\nGROUP BY DATE_TRUNC('MONTH', migration_date);",
  "created": "2025-11-20 12:57:08.768000-08:00",
  "comment": null
}
```

**Databricks Output:**
```sql
CREATE OR REPLACE VIEW data_migration_db.data_migration_schema.data_migration_monthly_summary AS
SELECT 
    DATE_TRUNC('MONTH', migration_date) AS month,
    COUNT(DISTINCT target_id) AS target_count,
    COUNT(DISTINCT source_id) AS source_count,
    SUM(total_records) AS total_records_migrated,
    AVG(total_records) AS avg_records_per_target
FROM data_migration_db.data_migration_schema.data_migration_target
WHERE migration_status = 'COMPLETED'
GROUP BY DATE_TRUNC('MONTH', migration_date);
```

**Mapping Rule:** Direct equivalent - Views map directly. Note: ACL restrictions may need to be enforced via permissions.

---

## Data Transportation & Streaming

### Stage → Volume

**Snowflake Input:**
```json
{
  "created_on": "2025-11-20 13:05:15.203000-08:00",
  "name": "DATA_MIGRATION_SOURCE_STAGE",
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "url": "",
  "has_credentials": "N",
  "has_encryption_key": "N",
  "owner": "SYSADMIN",
  "comment": "Stage for data migration source files",
  "region": null,
  "type": "INTERNAL",
  "cloud": null,
  "notification_channel": null,
  "storage_integration": null,
  "endpoint": null,
  "owner_role_type": "ROLE",
  "directory_enabled": "Y"
}
```

**Databricks Output:**
```sql
CREATE VOLUME IF NOT EXISTS data_migration_db.data_migration_schema.data_migration_source_stage
COMMENT 'Stage for data migration source files';
-- Note: Schema scoping differences - Databricks volumes are scoped within catalogs/schemas
-- Internal stages map to volumes in Databricks
```

**Mapping Rule:** Imperfect match - Schema isn't automatically matched in Volumes. Schema scoping differences apply.

---

### Stream → Delta Change Data Feed

**Snowflake Input:**
```json
{
  "created_on": "2025-11-20 13:05:49.616000-08:00",
  "name": "DATA_MIGRATION_SOURCE_CHANGES",
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "owner": "SYSADMIN",
  "comment": "Stream to track data migration source table changes",
  "table_name": "DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.DATA_MIGRATION_SOURCE",
  "source_type": "Table",
  "base_tables": "DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.DATA_MIGRATION_SOURCE",
  "type": "DELTA",
  "stale": "false",
  "mode": "DEFAULT",
  "stale_after": "2025-12-05 08:22:43.058000-08:00",
  "invalid_reason": "N/A",
  "owner_role_type": "ROLE"
}
```

**Databricks Output:**
```sql
-- Enable Change Data Feed on the source table
ALTER TABLE data_migration_db.data_migration_schema.data_migration_source
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Create a view to read change data (equivalent to stream)
CREATE VIEW data_migration_db.data_migration_schema.data_migration_source_changes AS
SELECT * FROM table_changes('data_migration_db.data_migration_schema.data_migration_source', 0);
```

**Mapping Rule:** Direct equivalent - Streams map to Delta Change Data Feed.

---

### Pipe (Snowpipe) → Auto Loader

**Snowflake Input:**
```json
{
  "created_on": "2025-11-20 13:05:58.512000-08:00",
  "name": "DATA_MIGRATION_SOURCE_PIPE",
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "definition": "COPY INTO data_migration_source\n    FROM @data_migration_source_stage\n    FILE_FORMAT = data_migration_csv_format",
  "owner": "SYSADMIN",
  "notification_channel": "sfc-br-ds1-3-customer-stage/33za0000-s/",
  "comment": "",
  "integration": null,
  "pattern": null,
  "error_integration": null,
  "owner_role_type": "ROLE",
  "invalid_reason": null,
  "kind": "STAGE"
}
```

**Databricks Output:**
```sql
-- Create streaming table with Auto Loader
CREATE OR REFRESH STREAMING TABLE data_migration_db.data_migration_schema.data_migration_source_streaming
AS SELECT * FROM cloud_files(
  's3://bucket/path/',
  'csv',
  map(
    'header', 'true',
    'delimiter', ',',
    'skipRows', '1'
  )
);
-- Note: Maps Snowpipe COPY INTO from stage to Auto Loader cloud_files function
```

**Mapping Rule:** Direct equivalent - Snowpipe maps to Auto Loader.

---

## Programmatic & Logical Objects

### Stored Procedure (SQL) → Unity Catalog SQL Procedure

**Snowflake Input (Simple Procedure):**
```json
{
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "procedure_name": "DATA_MIGRATION_GET_TARGETS",
  "procedure_definition": "\n    SELECT \n        target_id,\n        migration_date,\n        migration_status,\n        total_records\n    FROM data_migration_target\n    WHERE source_id = source_id_param\n    ORDER BY migration_date DESC;\n",
  "created": "2025-11-20 13:02:56.806000-08:00",
  "last_altered": "2025-11-20 13:02:56.806000-08:00",
  "comment": null
}
```

**Databricks Output:**
```sql
CREATE PROCEDURE data_migration_db.data_migration_schema.data_migration_get_targets(
    source_id_param BIGINT
)
RETURNS TABLE (
    target_id BIGINT,
    migration_date DATE,
    migration_status STRING,
    total_records BIGINT
)
LANGUAGE SQL
RETURN
    SELECT 
        target_id,
        migration_date,
        migration_status,
        total_records
    FROM data_migration_db.data_migration_schema.data_migration_target
    WHERE source_id = source_id_param
    ORDER BY migration_date DESC;
```

---

**Snowflake Input (Complex Procedure with Variables):**
```json
{
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "procedure_name": "DATA_MIGRATION_UPDATE_STATUS",
  "procedure_definition": "\nDECLARE\n    current_success NUMBER;\n    current_error NUMBER;\n    new_success NUMBER;\n    new_error NUMBER;\nBEGIN\n    -- Get current counts\n    SELECT success_count, error_count INTO current_success, current_error\n    FROM data_migration_target\n    WHERE target_id = target_id_param;\n    \n    -- Calculate new counts\n    new_success := current_success + success_count_change;\n    new_error := current_error + error_count_change;\n    \n    -- Update counts\n    UPDATE data_migration_target\n    SET success_count = new_success,\n        error_count = new_error,\n        total_records = new_success + new_error,\n        updated_at = CURRENT_TIMESTAMP()\n    WHERE target_id = target_id_param;\n    \n    RETURN 'Status updated: Success=' || new_success || ', Error=' || new_error;\nEND;\n",
  "created": "2025-11-20 13:03:14.658000-08:00",
  "last_altered": "2025-11-20 13:03:14.658000-08:00",
  "comment": null
}
```

**Databricks Output:**
```sql
CREATE PROCEDURE data_migration_db.data_migration_schema.data_migration_update_status(
    target_id_param BIGINT,
    success_count_change INT,
    error_count_change INT
)
RETURNS STRING
LANGUAGE SQL
BEGIN
    DECLARE current_success INT;
    DECLARE current_error INT;
    DECLARE new_success INT;
    DECLARE new_error INT;
    
    -- Get current counts
    SELECT success_count, error_count INTO current_success, current_error
    FROM data_migration_db.data_migration_schema.data_migration_target
    WHERE target_id = target_id_param;
    
    -- Calculate new counts
    SET new_success = current_success + success_count_change;
    SET new_error = current_error + error_count_change;
    
    -- Update counts
    UPDATE data_migration_db.data_migration_schema.data_migration_target
    SET success_count = new_success,
        error_count = new_error,
        total_records = new_success + new_error,
        updated_at = CURRENT_TIMESTAMP()
    WHERE target_id = target_id_param;
    
    RETURN CONCAT('Status updated: Success=', CAST(new_success AS STRING), ', Error=', CAST(new_error AS STRING));
END;
```

**Mapping Rule:** Direct equivalent - SQL procedures map directly to Unity Catalog SQL procedures.

---

### UDF (SQL) → Unity Catalog SQL UDF

**Snowflake Input:**
```json
{
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "function_name": "DATA_MIGRATION_GET_TOP_SOURCES",
  "function_definition": "\n    SELECT \n        source_id,\n        source_name,\n        total_records_migrated\n    FROM (\n        SELECT \n            s.source_id,\n            s.source_name,\n            COALESCE(SUM(t.total_records), 0) AS total_records_migrated,\n            ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(t.total_records), 0) DESC) AS rn\n        FROM data_migration_source s\n        LEFT JOIN data_migration_target t ON s.source_id = t.source_id AND t.migration_status = 'COMPLETED'\n        GROUP BY s.source_id, s.source_name\n    )\n    WHERE rn <= limit_count\n    ORDER BY total_records_migrated DESC\n",
  "created": "2025-11-20 13:04:39.533000-08:00",
  "last_altered": "2025-11-20 13:04:39.533000-08:00",
  "comment": null
}
```

**Databricks Output:**
```sql
CREATE FUNCTION data_migration_db.data_migration_schema.data_migration_get_top_sources(limit_count INT)
RETURNS TABLE (
    source_id BIGINT,
    source_name STRING,
    total_records_migrated BIGINT
)
RETURN
    SELECT 
        source_id,
        source_name,
        total_records_migrated
    FROM (
        SELECT 
            s.source_id,
            s.source_name,
            COALESCE(SUM(t.total_records), 0) AS total_records_migrated,
            ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(t.total_records), 0) DESC) AS rn
        FROM data_migration_db.data_migration_schema.data_migration_source s
        LEFT JOIN data_migration_db.data_migration_schema.data_migration_target t 
            ON s.source_id = t.source_id AND t.migration_status = 'COMPLETED'
        GROUP BY s.source_id, s.source_name
    )
    WHERE rn <= limit_count
    ORDER BY total_records_migrated DESC;
```

**Mapping Rule:** Direct equivalent - SQL UDFs map directly to Unity Catalog SQL UDFs.

---

### Sequence → Alternative (Identity Column) (REVIEW)

**Snowflake Input:**
```json
{
  "name": "DATA_MIGRATION_TARGET_ID_SEQ",
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "next_value": 1,
  "interval": 1,
  "created_on": "2025-11-20 13:04:50.173000-08:00",
  "owner": "SYSADMIN",
  "comment": "Sequence for data migration target IDs",
  "owner_role_type": "ROLE",
  "ordered": "N"
}
```

**Databricks Output:**
```sql
-- Databricks doesn't support sequences like Snowflake
-- Alternative: Use IDENTITY column in table definition
-- For existing table, sequence usage would need to be replaced with:
-- 1. IDENTITY columns (for new tables)
-- 2. Application-generated IDs/UUIDs
-- 3. Manual sequence management via a sequence table

-- Example: If creating new table with sequence
CREATE TABLE data_migration_db.data_migration_schema.data_migration_target_new (
    target_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    ...
);
```

**Mapping Rule:** Imperfect match - Sequences not directly supported. Use IDENTITY columns or application logic.

---

### File Format → File Format

**Snowflake Input (CSV Format):**
```json
{
  "created_on": "2025-11-20 13:05:27.056000-08:00",
  "name": "DATA_MIGRATION_CSV_FORMAT",
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "type": "CSV",
  "owner": "SYSADMIN",
  "comment": "CSV file format for data migration",
  "format_options": "{\"TYPE\":\"CSV\",\"RECORD_DELIMITER\":\"\\n\",\"FIELD_DELIMITER\":\",\",\"FILE_EXTENSION\":null,\"SKIP_HEADER\":1,\"PARSE_HEADER\":false,\"DATE_FORMAT\":\"AUTO\",\"TIME_FORMAT\":\"AUTO\",\"TIMESTAMP_FORMAT\":\"AUTO\",\"BINARY_FORMAT\":\"HEX\",\"ESCAPE\":\"NONE\",\"ESCAPE_UNENCLOSED_FIELD\":\"\\\\\",\"TRIM_SPACE\":true,\"FIELD_OPTIONALLY_ENCLOSED_BY\":\"\\\"\",\"NULL_IF\":[\"\\\\N\"],\"COMPRESSION\":\"AUTO\",\"ERROR_ON_COLUMN_COUNT_MISMATCH\":false,\"VALIDATE_UTF8\":true,\"SKIP_BLANK_LINES\":false,\"REPLACE_INVALID_CHARACTERS\":false,\"EMPTY_FIELD_AS_NULL\":true,\"SKIP_BYTE_ORDER_MARK\":true,\"ENCODING\":\"UTF8\",\"MULTI_LINE\":true}",
  "owner_role_type": "ROLE"
}
```

**Databricks Output:**
```sql
CREATE FILE FORMAT IF NOT EXISTS data_migration_db.data_migration_schema.data_migration_csv_format
TYPE = CSV
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
TRIM_SPACE = true
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('\\N')
COMPRESSION = AUTO
ENCODING = UTF8
COMMENT 'CSV file format for data migration';
```

---

**Snowflake Input (JSON Format):**
```json
{
  "created_on": "2025-11-20 13:05:34.371000-08:00",
  "name": "DATA_MIGRATION_JSON_FORMAT",
  "database_name": "DATA_MIGRATION_DB",
  "schema_name": "DATA_MIGRATION_SCHEMA",
  "type": "JSON",
  "owner": "SYSADMIN",
  "comment": "JSON file format for data migration",
  "format_options": "{\"TYPE\":\"JSON\",\"FILE_EXTENSION\":null,\"DATE_FORMAT\":\"AUTO\",\"TIME_FORMAT\":\"AUTO\",\"TIMESTAMP_FORMAT\":\"AUTO\",\"BINARY_FORMAT\":\"HEX\",\"TRIM_SPACE\":false,\"NULL_IF\":[],\"COMPRESSION\":\"AUTO\",\"ENABLE_OCTAL\":false,\"ALLOW_DUPLICATE\":false,\"STRIP_OUTER_ARRAY\":true,\"STRIP_NULL_VALUES\":false,\"IGNORE_UTF8_ERRORS\":false,\"REPLACE_INVALID_CHARACTERS\":false,\"SKIP_BYTE_ORDER_MARK\":true,\"MULTI_LINE\":true}",
  "owner_role_type": "ROLE"
}
```

**Databricks Output:**
```sql
CREATE FILE FORMAT IF NOT EXISTS data_migration_db.data_migration_schema.data_migration_json_format
TYPE = JSON
DATE_FORMAT = 'AUTO'
TIME_FORMAT = 'AUTO'
TIMESTAMP_FORMAT = 'AUTO'
COMPRESSION = AUTO
STRIP_OUTER_ARRAY = true
MULTI_LINE = true
COMMENT 'JSON file format for data migration';
```

**Mapping Rule:** Direct equivalent - File formats map directly.

---

## Summary Table

| Snowflake Object | Databricks Equivalent | Mapping Type | Notes |
|-----------------|----------------------|--------------|-------|
| Database | Catalog | Direct | None |
| Schema | Schema | Direct | None |
| Table (BASE TABLE) | Managed Table | Direct | None |
| View (Non-materialized) | View | Direct | ACL restrictions optional |
| Stage (INTERNAL) | Volume | Imperfect | Schema scoping differences |
| Stream (DELTA) | Delta Change Data Feed | Direct | None |
| Pipe (Snowpipe) | Auto Loader | Direct | None |
| Stored Procedure (SQL) | Unity Catalog SQL Procedure | Direct | None |
| UDF (SQL) | Unity Catalog SQL UDF | Direct | None |
| Sequence | IDENTITY Column / Alternative | Imperfect | Sequences not directly supported |
| File Format (CSV) | File Format | Direct | None |
| File Format (JSON) | File Format | Direct | None |

---

## Notes

- **Direct Equivalent**: One-to-one mapping with no special considerations
- **Imperfect Match**: Requires additional handling, limitations, or alternative approaches
- All examples use the actual metadata structure format extracted from Snowflake
- Generated SQL follows Databricks Unity Catalog syntax
- Data type conversions are handled automatically by the translation prompts
- Object names are converted to lowercase following Databricks naming conventions
- Full catalog.schema.object qualification is used in Databricks SQL for clarity
