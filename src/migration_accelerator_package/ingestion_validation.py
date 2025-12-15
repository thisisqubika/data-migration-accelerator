"""
Entry point for Snowflake metadata validation.
Runs as a Databricks wheel task using 'snowflake-validator'.
"""

import json
from snowflake.snowpark import Session
from databricks.sdk.runtime import dbutils

from migration_accelerator_package.snowpark_utils import (
    build_snowflake_connection_params,
    get_uc_volume_path,
)

from migration_accelerator_package.artifact_validators import MetadataValidator
from migration_accelerator_package.constants import SnowflakeConfig
from migration_accelerator_package.logging_utils import get_app_logger
from migration_accelerator_package.artifact_validators import MetadataValidator
from migration_accelerator_package.constants import SnowflakeConfig


logger = get_app_logger("snowflake-validator")


def main():
    logger.info("SNOWFLAKE METADATA VALIDATION starting")

    connection_parameters = build_snowflake_connection_params()
    session = Session.builder.configs(connection_parameters).create()

    db = SnowflakeConfig.SNOWFLAKE_DATABASE.value
    schema = SnowflakeConfig.SNOWFLAKE_SCHEMA.value

    volume_path = get_uc_volume_path()
    logger.info(f"UC Volume Path: {volume_path}")


    validator = MetadataValidator(session, volume_path)

    logger.info("Loading extracted metadata")
    extracted = validator.load_all_artifacts()
    logger.info("✓ Loaded all JSON files")

    logger.info("Running completeness validation")
    completeness_report = validator.validate_completeness(extracted, db, schema)
    logger.info("✓ Completeness check done")

    logger.info("Running correctness checks")

    sample_tables = extracted["tables"]["tables"][:5]
    table_results = [
        validator.validate_table_definition(db, schema, t)
        for t in sample_tables
    ]

    sample_views = extracted["views"]["views"][:5]
    view_results = [
        validator.validate_view_definition(db, schema, v)
        for v in sample_views
    ]

    report = {
        "database": db,
        "schema": schema,
        "completeness": completeness_report,
        "correctness": {
            "tables": table_results,
            "views": view_results,
        }
    }

    logger.info("✓ Validation complete")
    logger.info(json.dumps(report, indent=2))

    output_path = f"{volume_path}/validation_report.json"
    dbutils.fs.put(output_path, json.dumps(report, indent=2), overwrite=True)
    logger.info(f"✓ Validation report saved to {output_path}")

    session.close()


if __name__ == "__main__":
    main()
