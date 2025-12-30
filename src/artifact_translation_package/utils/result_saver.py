"""
Result saving utilities for Migration Accelerator.

This module provides centralized result saving functionality for JSON and SQL outputs,
including evaluation results, translation results, and summaries.
"""

import os
import json
from typing import Dict, Any, Optional

from artifact_translation_package.utils.sql_file_writer import save_sql_files
from artifact_translation_package.utils.logger import get_logger


def save_json_results(
    result: Dict[str, Any],
    results_dir: str,
    logger
) -> None:
    """
    Save results as JSON file.
    
    Args:
        result: Translation results dictionary
        results_dir: Directory to save results in
        logger: Logger instance
    """
    output_path_full = os.path.join(results_dir, "results.json")
    with open(output_path_full, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("JSON results saved", {"path": output_path_full, "total_results": result.get("metadata", {}).get("total_results", 0)})


def save_evaluation_results(
    eval_results: Dict[str, Any],
    output_dir: str,
    logger
) -> None:
    """
    Save evaluation/validation results.
    
    Args:
        eval_results: Evaluation results dictionary
        output_dir: Directory to save in
        logger: Logger instance
    """
    eval_dir = os.path.join(output_dir, "evaluations")
    os.makedirs(eval_dir, exist_ok=True)
    eval_path = os.path.join(eval_dir, "evaluation_results.json")
    with open(eval_path, 'w', encoding='utf-8') as f:
        json.dump(eval_results, f, indent=2, default=str)
    logger.info("Evaluation/validation results saved", {"path": eval_path, "count": len(eval_results)})


def save_translation_results(
    result: Dict[str, Any],
    output_dir: str,
    logger
) -> None:
    """
    Save main translation results.
    
    Args:
        result: Translation results dictionary
        output_dir: Directory to save in
        logger: Logger instance
    """
    translation_path = os.path.join(output_dir, "translation_results.json")
    with open(translation_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("Translation results saved", {"path": translation_path})


def save_results_summary(
    summary: Dict[str, Any],
    output_dir: str,
    logger
) -> None:
    """
    Save results summary/observability.
    
    Args:
        summary: Summary dictionary
        output_dir: Directory to save in
        logger: Logger instance
    """
    summary_path = os.path.join(output_dir, "results_summary.json")
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info("Results summary saved", {"path": summary_path})


def save_results(
    result: Dict[str, Any],
    output_path: str,
    output_format: str,
    context: Dict[str, Any],
    logger
) -> Optional[str]:
    """
    Save results based on output format.
    
    Args:
        result: Translation results dictionary
        output_path: Output path for results
        output_format: Output format (json/sql)
        context: Context dictionary
        logger: Logger instance
        
    Returns:
        Output directory path or None
    """
    results_dir = context.get("results_dir")
    if not results_dir:
        return None
    
    use_dbutils = context.get("using_dbutils", False) 
    
    if output_format == "json":
        save_json_results(result, results_dir, logger)
    elif output_format == "sql":
        save_sql_files(result, results_dir, use_dbutils=use_dbutils, logger=logger)
    else:
        return None
    
    # Persist evaluation/validation results
    eval_results = result.get("evaluation_results")
    if eval_results:
        save_evaluation_results(eval_results, results_dir, logger)
    
    # Persist main translation results
    save_translation_results(result, results_dir, logger)
    
    # Persist summary/observability if present
    summary = result.get("observability")
    if summary:
        save_results_summary(summary, results_dir, logger)
    
    return results_dir