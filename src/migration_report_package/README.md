# Migration Report Graph

A LangGraph-based system for creating a migration report based on the translation and evaluation results obtained from the DDL Translation Graph. 

## Project Structure

```
migration_report_package/
├── graph_builder.py           # LangGraph construction
├── main.py                    # Main entry point for the migration report
├── report_llm.py              # LLM configuration for report generation
```

## Usage

Process JSON files where each file contains a specific artifact type. The artifact type is determined from the filename (e.g., `tables.json`, `views.json`).

```bash
# Search for the "output" folder on the Default location and save the report there
python main.py

# Search for the "output" folder on the Default location and save the report on a custom location
python main.py --md_output ./custom_location

# Search for the "output" folder save the report on a custom location
python main.py --output_dir ./custom_location --md_output ./custom_location
```

### Programmatic Usage

```python
from graph_builder import MigrationReportGraph

# Create report from "output" folder in input_dir and save it in output_dir
graph = MigrationReportGraph()
md_report, json_report = graph.run(input_dir)

with open(os.path.join(output_dir, "migration_report.md"), "w", encoding="utf-8") as f:
    f.write(md_report)

print("JSON Report: ",json_report)
```

## Migration Report Example

```markdown
# Migration Report
## Overview
The migration process has completed with the following summary:
- Total artifacts migrated: 8
- Total errors: 0
- Total warnings: 0
- Total successes: 8
- Validation errors: 2

## Detailed Results per Artifact Type

### Schemas
| Artifact Name | Type | Status |
| --- | --- | --- |
| BRONZE_LAYER | schemas | success |
| SILVER_LAYER | schemas | success |
| GOLD_LAYER | schemas | success |

### Tables
| Artifact Name | Type | Status |
| --- | --- | --- |
| EXAMPLE_TABLE_1 | tables | error |
| EXAMPLE_TABLE_2 | tables | error |

### Views
| Artifact Name | Type | Status |
| --- | --- | --- |
| ACTIVE_USERS_VIEW | views | success |
| SALES_SUMMARY_VIEW | views | success |
| INVENTORY_STATUS_VIEW | views | success |

## Error and Warning Sections

### Errors
No errors were reported during the migration.

### Warnings
No warnings were reported during the migration.

## Objects Requiring Manual Review
The following objects require manual review due to validation errors or other issues:
- EXAMPLE_TABLE_1 (tables): Validation failed with syntax error.
- EXAMPLE_TABLE_2 (tables): Validation failed with syntax error.

## Summary of AI-assisted vs Rule-based Outputs
The migration utilized a combination of AI-assisted and rule-based approaches. The exact distribution is not available in the provided data.

## Performance Metrics
- Total duration: 31.17 seconds
- Stage durations:
  - translate_tables: 22.95 seconds
  - translate_views: 5.57 seconds
  - translate_schemas: 2.44 seconds

## Analysis

### Common Translation Errors
- The translation of tables resulted in Python code that failed validation due to syntax errors.

### Patterns in Warnings or Inconsistencies
- No warnings were reported, but the validation errors for tables indicate a potential inconsistency in the translation process.

### Success Rate per Artifact Type
- Schemas: 100% success (3/3)
- Tables: 0% success (0/2) due to validation errors
- Views: 100% success (3/3)

### Unsupported or Partially Supported Features
- The translation process generated Python code for tables, which is not directly executable in Databricks. This indicates a potential gap in the translation rules for tables.

### Dependencies that Failed or Were Skipped
- The tables (EXAMPLE_TABLE_1 and EXAMPLE_TABLE_2) failed due to validation errors, indicating potential issues with dependencies or the translation process.

### Recommendations for Improving Translation Rules
1. Review and adjust the translation rules for tables to directly generate valid Databricks DDL instead of Python code.
2. Enhance the validation step to catch syntax errors early in the translation process.

### Suggested Workaround for Unsupported Features
For tables, manually review and convert the generated Python code into valid Databricks DDL statements. Ensure that the data types and constraints are correctly translated.
```

## Requirements

- Python 3.7+
- LangChain ecosystem
