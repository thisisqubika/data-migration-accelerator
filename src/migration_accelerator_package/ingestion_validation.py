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


def main():
    print("=" * 80)
    print(" SNOWFLAKE METADATA VALIDATION ")
    print("=" * 80)

    connection_parameters = build_snowflake_connection_params()
    session = Session.builder.configs(connection_parameters).create()

    db = SnowflakeConfig.SNOWFLAKE_DATABASE.value
    schema = SnowflakeConfig.SNOWFLAKE_SCHEMA.value

    volume_path = get_uc_volume_path()
    print(f"UC Volume Path: {volume_path}")


    validator = MetadataValidator(session, volume_path)

    print("Loading extracted metadata...")
    extracted = validator.load_all_artifacts()
    print("✓ Loaded all JSON files")

    print("Running completeness validation...")
    completeness_report = validator.validate_completeness(extracted, db, schema)
    print("✓ Completeness check done")

    print("Running correctness checks...")

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

    print("✓ Validation complete")
    print(json.dumps(report, indent=2))

    output_path = f"{volume_path}/validation_report.json"
    dbutils.fs.put(output_path, json.dumps(report, indent=2), overwrite=True)
    print(f"✓ Validation report saved to {output_path}")

    session.close()


if __name__ == "__main__":
    main()
