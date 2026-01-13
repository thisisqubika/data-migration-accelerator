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
## 1. EXECUTIVE SUMMARY

The migration process has successfully translated 8 artifacts (3 schemas, 3 views, and 2 tables) from Snowflake to Databricks Unity Catalog. However, the translation of tables resulted in Python code, which failed Databricks validation, leading to 2 validation errors. Despite these errors, schemas and views were migrated successfully. The project is partially deployable, with schemas and views being usable. Manual intervention is required for tables to fix the validation errors. This result is expected due to the current misalignment of table translation rules with Databricks DDL requirements.

<br>

## 2. OVERVIEW

- Artifacts Processed: 8 (3 schemas, 3 views, 2 tables)
- Migration Errors: 0
- Validation Errors: 2
- Migration Warnings: 0
- Overall Status: Partial Success (schemas and views completed; tables require fixes)

The project is considered partially deployable because while schemas and views are successfully translated and validated, tables require manual intervention to fix validation errors. This is expected due to the current limitations in table translation logic. The workspace characteristics, specifically the use of Python code for table translations, contribute to this outcome.

> **Error Severity Guide**                                                            
> ------------------------------
>  \[CRITICAL\]  Migration Error     : Translation or execution failed.                        
>  \[BLOCKER\]   Validation Error    : Translation completed but failed validation.            
>  \[WARNING\]   Migration Warning   : Non-blocking issues; artifact is deployable.   

<br>

## 3. OBJECTS REQUIRING MANUAL REVIEW

The tables require manual review to fix the validation errors caused by the Python code generated during translation. The user is expected to manually correct the table DDL to comply with Databricks requirements. There is no suggested workaround provided by the migration tool for this issue, and it is considered within the scope of manual intervention required for deployment. This issue will block the general deployment of tables but not schemas and views.

<br>

## 4. DETAILED RESULTS PER ARTIFACT TYPE

| Artifact Name | Type | Status | Issue |
| --- | --- | --- | --- |
| DATA_MIGRATION_DB.BRONZE_LAYER | Schema | Success | - |
| DATA_MIGRATION_DB.SILVER_LAYER | Schema | Success | - |
| DATA_MIGRATION_DB.GOLD_LAYER | Schema | Success | - |
| DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.ACTIVE_USERS_VIEW | View | Success | - |
| DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.SALES_SUMMARY_VIEW | View | Success | - |
| DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.INVENTORY_STATUS_VIEW | View | Success | - |
| DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.EXAMPLE_TABLE_1 | Table | Validation Error | Syntax Error |
| DATA_MIGRATION_DB.DATA_MIGRATION_SCHEMA.EXAMPLE_TABLE_2 | Table | Validation Error | Syntax Error |

> **STATUS & SEVERITY GUIDE**                                                              
> ------------------------------
>  \[SUCCESS\]   Success            : Artifact translated and validated successfully.                       
>  \[CRITICAL\]  Translation Error  : Artifact was not translated.                       
>  \[BLOCKER\]   Validation Error   : Translation completed but failed validation.            
>  \[WARNING\]   Warning            : Translated with non-blocking issues or limitations.  

<br>

## 5. MIGRATION ERRORS AND WARNINGS

There were no migration errors or warnings reported during the migration process.

<br>

## 6. ANALYSIS

### 6.1. KEY FINDINGS
- Schema and view translations are stable and validate successfully.
- Table translation logic currently produces Python code, which fails Databricks validation.

### 6.2. ROOT CAUSE
- Table translation rules are misaligned with Databricks DDL requirements.

### 6.3. IMPACT
- Tables require manual intervention before deployment.
- No impact on schemas or views.

<br>

## 7. FURTHER ANALYSIS

- Common translation errors: None reported.
- Patterns in warnings or inconsistencies: The generation of Python code for tables instead of valid Databricks DDL.
- Success rate per artifact type: Schemas and views have a 100% success rate; tables have a 0% success rate due to validation errors.
- Unsupported or partially supported features: Table translations that result in Python code are not supported as-is in Databricks.
- Dependencies that failed or were skipped: None reported.
- Recommendations for improving translation rules: Align table translation logic with Databricks DDL requirements.
- Suggested workaround for unsupported features: Manual correction of table DDL.

<br>

## 8. PERFORMANCE METRICS

- Total Duration: 		31.17 seconds
- Average Time per Artifact: 	~3.9 seconds
- Pipeline Execution: 		Completed successfully
- Retries: 			0
```

## Requirements

- Python 3.7+
- LangChain ecosystem
