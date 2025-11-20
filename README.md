# Data Migration Accelerator

A project for testing and validating Snowflake to Databricks migration tools and extractors.

## Overview

This project contains Snowflake test objects that can be used to validate extractors and conversion tools that migrate Snowflake database objects to Databricks.

## Files

- **snowflake_test_objects.sql** - Contains sample Snowflake objects (tables, views, procedures, functions, etc.) with the `data_migration` naming convention for testing migration tools.

## Usage

1. Execute `snowflake_test_objects.sql` in your Snowflake environment to create the test objects
2. Use your migration tool/extractor to convert these Snowflake objects to Databricks
3. Validate the converted objects in Databricks

## Test Objects

The SQL file includes:

- **Tables**: `data_migration_source`, `data_migration_target`
- **Views**: Various views for data migration summaries and status
- **Stored Procedures**: Procedures for querying migration data
- **User-Defined Functions**: Scalar and table functions
- **Other Objects**: Sequences, stages, file formats, tasks, streams, and pipes

## Configuration

- **Database**: `DATA_MIGRATION_DB`
- **Schema**: `DATA_MIGRATION_SCHEMA`
- **Data Retention**: 1 day (max retention)
