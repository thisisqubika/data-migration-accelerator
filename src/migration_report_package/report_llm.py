import json
from typing import Any, Dict, List, Optional, Annotated, TypedDict


llm_config = {
                "provider": "databricks",
                "model": "databricks-llama-4-maverick",
                "temperature": 0.2,
                "max_tokens": 4000,
                "additional_params": {
                    "endpoint": "databricks-llama-4-maverick"
                }
            }

PROMPT = """You are an expert in migrating Snowflake databases to Databricks Unity Catalog.

The migration is done, your task is now to create a migration report accroding to the migration output received. 

MIGRATION OUTPUT STRUCTURE:
The migration output will include:
- transaltion results: information about the migrated artifacts, observability metrics
- evaluation: evaluations on the migration results
- count: count of total migrated artifacts, total errors, total warnings, total successes

REPORT STRUCTURE:

The report must be returned in MARKDOWN format. Make all H2 and H3 headers UPPERCASE and numbered. Between each H2 header and its content add a line break. Where possible, introduce tabs or visual separation between sections to enhance scanability. 

The report must include the following items and sections:

- First a short executive summary stating in words: what got migrated, what requires manual intervention, whether the project is deployable or not (even if there are errors on some artifacts).

- ## Overview: Summary of migration process, always provide a numeric summary following the example below:
   ---------
    - Artifacts Processed: 8 (3 schemas, 2 tables, 2 views)
    - Migration Errors: 0
    - Validation Errors: 2
    - Migration Warnings: 0
    - Overall Status: Partial Success (schemas and views completed; tables require fixes)
   ---------
If "Partial Success" is returned, add a few lines saying if the project is still acceptable to go live and what is missing to achieve sucess, also, if applicable, answer, is this result exprected because of some characteristic of the workspace?
Below the summary above include the following table, make sure to align the colons and the three columns. 

>  **Error Severity Guide**                                                            
> ------------------------------
>  \[CRITICAL\]  Migration Error     : Translation or execution failed.                        
>  \[BLOCKER\]   Validation Error    : Translation completed but failed validation.            
>  \[WARNING\]   Migration Warning   : Non-blocking issues; artifact is deployable.   

- ## Objects requiring manual review: For every action that requires manual review add what is the user expected to do, if there is a suggested workaround, if it is out of scope, or if this is something a future version of the accelerator could cover. Also add if this issue will block the general deloyment and if deployable, what is usable and what isn't.
- ## Detailed results per artifact type: Every entry must include: artifact name, type, status [one of: Success, Validation Error, Translation Error, Warning] and Issue. In the Issue column, if the status is error assign it to one of this categories: Syntax Error, LLM Error, Task Failure. If no error just add "-". Below the table add the following explanation, make sure to align the colons and the three columns.

> **STATUS & SEVERITY GUIDE**                                                              
> ------------------------------
>  \[SUCCESS\]   Success            : Artifact translated and validated successfully.                       
>  \[CRITICAL\]  Translation Error  : Artifact was not translated.                       
>  \[BLOCKER\]   Validation Error   : Translation completed but failed validation.            
>  \[WARNING\]   Warning            : Translated with non-blocking issues or limitations.  


- ## Migration Errors and Warnings
- ## Analysis: Section that must include the following subsections:
    - Key Findings
    - Root Cause
    - Impact
    An example of this section is:
    ---------
    ### Key Findings
    - Schema and view translations are stable and validate successfully.
    - Table translation logic currently produces Python code, which fails Databricks validation.
    ### Root Cause
    - Table translation rules are misaligned with Databricks DDL requirements.
    ### Impact
    - Tables require manual intervention before deployment.
    - No impact on schemas or views.
    ---------
- ## Further Analysis: Make sure to include the following:
    - Common translation errors
    - Patterns in warnings or inconsistencies
    - Success rate per artifact type
    - Unsupported or partially supported features
    - Dependencies that failed or were skipped
    - Recommendations for improving translation rules
    - Suggested workaround for unsupported features
- ## Performance metrics: include total duration, average time per artifact, pipeline execution status, retries, timeouts. Format it like this example, both columns should be aligned:
  ---------
- Total Duration: 		31.17 seconds
- Average Time per Artifact: 	~3.9 seconds
- Pipeline Execution: 		Completed successfully
- Retries: 			0
  ---------

The migration output is:
Count: {count}
evaluation: {evaluation}
translation results: {translation_results}
"""

def create_node_llm(node_name: str, llm_config: Dict[str, Any]):
    try:
        from databricks_langchain import ChatDatabricks
    except ImportError:
        try:
            from langchain_databricks import ChatDatabricks
        except ImportError:
            from langchain_community.chat_models import ChatDatabricks

    return ChatDatabricks(
        endpoint=llm_config.get("additional_params", {}).get("endpoint", "databricks-llama-4-maverick"),
        temperature=llm_config.get("temperature"),
        max_tokens=llm_config.get("max_tokens") or 2000,
    )

def generate_report(data: Dict[str, Any], count: Dict[str, Any]) -> List[str]:
    """
    Args: 
        data: Dictionary with translation and evaluation results
        count: Dictionary with count of artifacts, errors, warnings and validation errors

    Returns:
        result of the LLM call
    """
    llm = create_node_llm("report_node", llm_config=llm_config)
    results = []

    prompt = PROMPT.format(count=count, evaluation=data["evaluation"], translation_results=data["translation_results"])

    try:
        response = llm.invoke(prompt)
        response = response.content if hasattr(response, 'content') else str(response)
        results.append(response.strip())
    except Exception as e:
        error_msg = f"LLM error for migration report: {str(e)}"
        results.append(f"-- Error generating migration report: {str(e)}")

    return results
