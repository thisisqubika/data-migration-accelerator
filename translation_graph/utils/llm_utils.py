import os
from typing import Dict, Any
import time

from config.ddl_config import create_node_llm, get_config


def create_llm_for_node(node_name: str):
    return create_node_llm(node_name)


def validate_node_requirements() -> Dict[str, Any]:
    validation_results = {
        "llm_available": False,
        "environment_ready": False,
        "errors": []
    }

    try:
        llm = create_llm_for_node("smart_router")
        validation_results["llm_available"] = True
    except Exception as e:
        validation_results["errors"].append(f"LLM creation failed: {e}")

    dbx_endpoint = os.getenv("DBX_ENDPOINT")
    if not dbx_endpoint:
        validation_results["errors"].append("DBX_ENDPOINT environment variable not set")
    else:
        validation_results["environment_ready"] = True

    return validation_results


def test_llm_connection() -> Dict[str, Any]:
    test_results = {
        "connection_successful": False,
        "response_time": None,
        "error": None,
        "test_message": "Hello, this is a test."
    }

    try:
        start_time = time.time()

        llm = create_llm_for_node("smart_router")
        response = llm.invoke("Say 'Hello' if you can read this.")

        end_time = time.time()
        test_results["response_time"] = end_time - start_time
        test_results["connection_successful"] = True
        test_results["response"] = response.content if hasattr(response, 'content') else str(response)

    except Exception as e:
        test_results["error"] = str(e)

    return test_results
