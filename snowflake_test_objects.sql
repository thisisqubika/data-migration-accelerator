-- ============================================================================
-- Snowflake Test Objects for Databricks Extractor Testing
-- ============================================================================
-- This file contains sample Snowflake objects (tables, views, procedures, etc.)
-- that can be used to test an extractor that converts Snowflake artifacts to Databricks
-- ============================================================================

-- Set context
USE DATABASE DATA_MIGRATION_DB;
USE SCHEMA DATA_MIGRATION_SCHEMA;

-- ============================================================================
-- TABLES
-- ============================================================================

-- Sample table with various data types
CREATE OR REPLACE TABLE data_migration_source (
    source_id NUMBER(38, 0) PRIMARY KEY,
    source_name VARCHAR(100),
    source_type VARCHAR(100),
    source_connection VARCHAR(255),
    source_status VARCHAR(50),
    migration_timestamp TIMESTAMP_NTZ(9),
    is_active BOOLEAN,
    record_count DECIMAL(18, 0),
    metadata VARIANT,
    tags ARRAY,
    config OBJECT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Data migration source table with various data types';

-- Sample table with clustering and foreign key
CREATE OR REPLACE TABLE data_migration_target (
    target_id NUMBER(38, 0) PRIMARY KEY,
    source_id NUMBER(38, 0) REFERENCES data_migration_source(source_id),
    target_name VARCHAR(200) NOT NULL,
    target_type VARCHAR(100),
    migration_date DATE,
    migration_status VARCHAR(50),
    total_records DECIMAL(18, 0),
    success_count NUMBER(10, 0) DEFAULT 0,
    error_count NUMBER(10, 0) DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    migration_config VARIANT,
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (migration_date, migration_status)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Data migration target table with clustering and time travel';

-- ============================================================================
-- VIEWS
-- ============================================================================

-- Simple view
CREATE OR REPLACE VIEW data_migration_active_sources AS
SELECT 
    source_id,
    source_name,
    source_type,
    source_status,
    record_count,
    created_at
FROM data_migration_source
WHERE is_active = TRUE
COMMENT = 'View of active data migration sources only';

-- Complex view with joins
CREATE OR REPLACE VIEW data_migration_summary AS
SELECT 
    s.source_id,
    s.source_name,
    s.source_type,
    COUNT(t.target_id) AS total_targets,
    SUM(t.total_records) AS total_records_migrated,
    MAX(t.migration_date) AS last_migration_date,
    AVG(t.total_records) AS avg_records_per_target
FROM data_migration_source s
LEFT JOIN data_migration_target t ON s.source_id = t.source_id
WHERE s.is_active = TRUE
GROUP BY s.source_id, s.source_name, s.source_type
COMMENT = 'Data migration summary view';

-- View with window functions
CREATE OR REPLACE VIEW data_migration_status_ranked AS
SELECT 
    t.target_id,
    t.target_name,
    t.target_type,
    t.total_records,
    t.success_count,
    t.error_count,
    RANK() OVER (PARTITION BY t.target_type ORDER BY t.total_records DESC) AS type_rank,
    RANK() OVER (ORDER BY t.total_records DESC) AS overall_rank
FROM data_migration_target t
WHERE t.migration_status = 'COMPLETED';

-- Materialized view (if supported)
CREATE OR REPLACE VIEW data_migration_monthly_summary AS
SELECT 
    DATE_TRUNC('MONTH', migration_date) AS month,
    COUNT(DISTINCT target_id) AS target_count,
    COUNT(DISTINCT source_id) AS source_count,
    SUM(total_records) AS total_records_migrated,
    AVG(total_records) AS avg_records_per_target
FROM data_migration_target
WHERE migration_status = 'COMPLETED'
GROUP BY DATE_TRUNC('MONTH', migration_date);

-- ============================================================================
-- STORED PROCEDURES
-- ============================================================================

-- Simple stored procedure
CREATE OR REPLACE PROCEDURE data_migration_get_targets(source_id_param NUMBER)
RETURNS TABLE (target_id NUMBER, migration_date DATE, migration_status VARCHAR, total_records DECIMAL)
LANGUAGE SQL
AS
$$
    SELECT 
        target_id,
        migration_date,
        migration_status,
        total_records
    FROM data_migration_target
    WHERE source_id = source_id_param
    ORDER BY migration_date DESC;
$$

-- Stored procedure with parameters and logic
CREATE OR REPLACE PROCEDURE data_migration_update_status(
    target_id_param NUMBER,
    success_count_change NUMBER,
    error_count_change NUMBER
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_success NUMBER;
    current_error NUMBER;
    new_success NUMBER;
    new_error NUMBER;
BEGIN
    -- Get current counts
    SELECT success_count, error_count INTO current_success, current_error
    FROM data_migration_target
    WHERE target_id = target_id_param;
    
    -- Calculate new counts
    new_success := current_success + success_count_change;
    new_error := current_error + error_count_change;
    
    -- Update counts
    UPDATE data_migration_target
    SET success_count = new_success,
        error_count = new_error,
        total_records = new_success + new_error,
        updated_at = CURRENT_TIMESTAMP()
    WHERE target_id = target_id_param;
    
    RETURN 'Status updated: Success=' || new_success || ', Error=' || new_error;
END;
$$

-- ============================================================================
-- USER-DEFINED FUNCTIONS (UDFs)
-- ============================================================================

-- Scalar UDF (SQL)
CREATE OR REPLACE FUNCTION data_migration_calculate_success_rate(
    success_count DECIMAL,
    total_count NUMBER
)
RETURNS DECIMAL(18, 2)
LANGUAGE SQL
AS
$$
    SELECT CASE 
        WHEN total_count > 0 THEN (success_count / total_count) * 100
        ELSE 0
    END
$$;

-- Table UDF
CREATE OR REPLACE FUNCTION data_migration_get_top_sources(
    limit_count NUMBER
)
RETURNS TABLE (source_id NUMBER, source_name VARCHAR, total_records_migrated DECIMAL)
LANGUAGE SQL
AS
$$
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
        FROM data_migration_source s
        LEFT JOIN data_migration_target t ON s.source_id = t.source_id AND t.migration_status = 'COMPLETED'
        GROUP BY s.source_id, s.source_name
    )
    WHERE rn <= limit_count
    ORDER BY total_records_migrated DESC
$$;

-- ============================================================================
-- SEQUENCES
-- ============================================================================

CREATE OR REPLACE SEQUENCE data_migration_source_id_seq
    START WITH 1000
    INCREMENT BY 1
    COMMENT = 'Sequence for data migration source IDs';

CREATE OR REPLACE SEQUENCE data_migration_target_id_seq
    START WITH 1
    INCREMENT BY 1
    COMMENT = 'Sequence for data migration target IDs';

-- ============================================================================
-- STAGES (for file loading)
-- ============================================================================

-- Internal stage
CREATE OR REPLACE STAGE data_migration_source_stage
    COMMENT = 'Stage for data migration source files';

-- External stage (example - adjust for your S3/GCS setup)
-- CREATE OR REPLACE STAGE data_migration_external_stage
--     URL = 's3://your-bucket/data-migration/'
--     CREDENTIALS = (AWS_KEY_ID = 'your-key' AWS_SECRET_KEY = 'your-secret')
--     COMMENT = 'External stage for data migration files';

-- ============================================================================
-- FILE FORMATS
-- ============================================================================

CREATE OR REPLACE FILE FORMAT data_migration_csv_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'CSV file format for data migration';

CREATE OR REPLACE FILE FORMAT data_migration_json_format
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON file format for data migration';

-- ============================================================================
-- TASKS (for scheduling)
-- ============================================================================

-- Task to update daily migration summary
CREATE OR REPLACE TASK data_migration_update_daily_summary
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 0 * * * UTC'
    COMMENT = 'Daily task to update data migration summary'
AS
    INSERT INTO data_migration_daily_summary
    SELECT 
        CURRENT_DATE() AS migration_date,
        COUNT(*) AS target_count,
        SUM(total_records) AS total_records_migrated
    FROM data_migration_target
    WHERE migration_date = CURRENT_DATE() - 1;

-- ============================================================================
-- STREAMS (for change data capture)
-- ============================================================================

CREATE OR REPLACE STREAM data_migration_source_changes ON TABLE data_migration_source
    COMMENT = 'Stream to track data migration source table changes';

-- ============================================================================
-- PIPES (for continuous data loading)
-- ============================================================================

-- Example pipe (requires external stage)
CREATE OR REPLACE PIPE data_migration_source_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO data_migration_source
    FROM @data_migration_source_stage
    FILE_FORMAT = data_migration_csv_format;

-- ============================================================================
-- GRANTS (permissions)
-- ============================================================================

-- Grant usage on schema
-- GRANT USAGE ON SCHEMA DATA_MIGRATION_SCHEMA TO ROLE ANALYST_ROLE;

-- -- Grant select on tables
-- GRANT SELECT ON ALL TABLES IN SCHEMA DATA_MIGRATION_SCHEMA TO ROLE ANALYST_ROLE;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA DATA_MIGRATION_SCHEMA TO ROLE ANALYST_ROLE;

-- -- Grant execute on procedures
-- GRANT USAGE ON PROCEDURE data_migration_get_targets(NUMBER) TO ROLE ANALYST_ROLE;

-- -- ============================================================================
-- -- SAMPLE DATA INSERTION
-- -- ============================================================================

-- -- Insert sample data migration sources
-- INSERT INTO data_migration_source (source_id, source_name, source_type, source_connection, source_status, migration_timestamp, is_active, record_count)
-- VALUES
--     (1, 'Legacy Database', 'SQL_SERVER', 'sqlserver://legacy-db:1433', 'ACTIVE', '2024-01-15 10:00:00', TRUE, 150000),
--     (2, 'Oracle Warehouse', 'ORACLE', 'oracle://warehouse:1521', 'ACTIVE', '2024-01-16 11:00:00', TRUE, 230000),
--     (3, 'PostgreSQL Archive', 'POSTGRESQL', 'postgresql://archive:5432', 'INACTIVE', '2024-01-17 12:00:00', FALSE, 50000);

-- -- Insert sample data migration targets
-- INSERT INTO data_migration_target (target_id, source_id, target_name, target_type, migration_date, migration_status, total_records, success_count, error_count, is_active)
-- VALUES
--     (1001, 1, 'Databricks Delta Table', 'DELTA', '2024-01-15', 'COMPLETED', 150000, 149500, 500, TRUE),
--     (1002, 2, 'Databricks Parquet Files', 'PARQUET', '2024-01-16', 'COMPLETED', 230000, 229800, 200, TRUE),
--     (1003, 1, 'Databricks Bronze Layer', 'DELTA', '2024-01-20', 'PENDING', 0, 0, 0, TRUE);

-- ============================================================================
-- END OF TEST OBJECTS
-- ============================================================================

