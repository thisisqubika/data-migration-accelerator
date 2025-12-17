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

The report must be returned in MARKDOWN format.
The report must include the following sections:
- Overview
- Detailed results per artifact type. Every entry must include: artifact name, type, status [one of: success, error, warning]. If the status is error assign it to one of this categories: Syntax Error, LLM Error, Unsupported Feedback Error, Other Error 
- Error and warning sections
- Objects requiring manual review
- Summary of AI-assisted vs rule-based outputs
- Performance metrics

REPORT ANALYSIS:

Make sure to include the following:

- Common translation errors
- Patterns in warnings or inconsistencies
- Success rate per artifact type
- Unsupported or partially supported features
- Dependencies that failed or were skipped
- Recommendations for improving translation rules
- Suggested workaround for unsupported features

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

def generate_report(data: List[str], count: Dict[str, Any]) -> List[str]:
    """
    Args:

    Returns:
        
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
