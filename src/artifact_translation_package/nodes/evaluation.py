"""
LLM-based SQL evaluation node.

This module provides LLM-based evaluation for complex SQL validation cases.
Currently NOT connected to the graph - reserved for future complex case handling.
"""

import os
import json
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional

try:
    from langchain_core.output_parsers import PydanticOutputParser
except ImportError:
    PydanticOutputParser = None

from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.llm_utils import create_llm_for_node
from artifact_translation_package.prompts.evaluation_prompts import EvaluationPrompts
from artifact_translation_package.utils.evaluation_models import SQLEvaluationResult, SQLIssue, BatchSQLEvaluationResult
from artifact_translation_package.config.ddl_config import get_config
import logging

# Module logger
logger = logging.getLogger(__name__)


def create_structured_llm(llm, batch_mode: bool = False):
    """
    Create a structured output LLM using Pydantic model.
    
    Args:
        llm: Base LLM instance
        batch_mode: If True, use BatchSQLEvaluationResult for batch processing
        
    Returns:
        LLM configured for structured output
    """
    model = BatchSQLEvaluationResult if batch_mode else SQLEvaluationResult
    
    try:
        if hasattr(llm, 'with_structured_output'):
            return llm.with_structured_output(model)
    except Exception:
        pass
    
    try:
        if PydanticOutputParser is not None:
            parser = PydanticOutputParser(pydantic_object=model)
            return llm | parser
    except Exception:
        pass
    
    return llm


def get_evaluation_batch_size() -> int:
    """
    Get the batch size for SQL evaluation.
    
    Returns:
        Number of SQL statements to evaluate in a single LLM call
    """
    config = get_config()
    return config.get("processing", {}).get("evaluation_batch_size", 5)


def clean_sql_preview(sql_statement: str, max_length: int = 200) -> str:
    """
    Clean SQL statement for preview display in JSON.
    Removes markdown code blocks and normalizes whitespace.
    
    Args:
        sql_statement: Original SQL statement
        max_length: Maximum length of preview
        
    Returns:
        Cleaned SQL preview string
    """
    cleaned = sql_statement.strip()
    
    if cleaned.startswith("```sql"):
        cleaned = cleaned[6:].strip()
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:].strip()
    
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3].strip()
    
    cleaned = cleaned.replace("\n", " ").replace("\r", " ")
    cleaned = " ".join(cleaned.split())
    
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length] + "..."
    
    return cleaned


def create_error_evaluation_result(error_message: str) -> SQLEvaluationResult:
    """
    Create an error evaluation result for evaluation failures.
    
    Args:
        error_message: Error message describing the failure
        
    Returns:
        SQLEvaluationResult with error information
    """
    return SQLEvaluationResult(
        is_compliant=False,
        compliance_score=0.0,
        issues=[
            SQLIssue(
                issue_type="other",
                severity="error",
                description=f"Evaluation failed: {error_message}",
                suggestion="Check LLM response and configuration"
            )
        ],
        syntax_valid=False,
        follows_best_practices=False,
        summary=f"Evaluation error: {error_message}"
    )


def parse_evaluation_response(response: Any, sql_statement: str = None) -> SQLEvaluationResult:
    """
    Extract evaluation result from structured output response.
    
    With structured outputs, the LLM returns a SQLEvaluationResult directly.
    
    Args:
        response: Structured output response (SQLEvaluationResult instance)
        sql_statement: Original SQL statement being evaluated (for context, optional)
        
    Returns:
        SQLEvaluationResult object
    """
    if isinstance(response, SQLEvaluationResult):
        return response
    
    raise TypeError(f"Expected SQLEvaluationResult, got {type(response)}")


def parse_batch_evaluation_response(response: Any) -> List[SQLEvaluationResult]:
    """
    Extract evaluation results from structured output response.
    
    With structured outputs, the LLM returns a BatchSQLEvaluationResult directly,
    so we just extract the results list.
    
    Args:
        response: Structured output response (BatchSQLEvaluationResult instance)
        
    Returns:
        List of SQLEvaluationResult objects
    """
    if isinstance(response, BatchSQLEvaluationResult):
        return response.results
    
    raise TypeError(f"Expected BatchSQLEvaluationResult, got {type(response)}")


def create_issue_summary(
    sql_index: int,
    sql_statement: str,
    evaluation_result: SQLEvaluationResult
) -> Dict[str, Any]:
    """
    Create a summary dictionary for a non-compliant SQL statement.
    
    Args:
        sql_index: Index of the SQL statement in the batch
        sql_statement: The SQL statement that failed
        evaluation_result: The evaluation result for this statement
        
    Returns:
        Dictionary containing issue summary
    """
    return {
        "sql_index": sql_index,
        "sql_preview": clean_sql_preview(sql_statement),
        "is_compliant": evaluation_result.is_compliant,
        "compliance_score": evaluation_result.compliance_score,
        "syntax_valid": evaluation_result.syntax_valid,
        "follows_best_practices": evaluation_result.follows_best_practices,
        "summary": evaluation_result.summary,
        "detected_object_type": evaluation_result.detected_object_type,
        "issues": [
            {
                "type": issue.issue_type,
                "severity": issue.severity,
                "description": issue.description,
                "line_number": issue.line_number,
                "suggestion": issue.suggestion
            }
            for issue in evaluation_result.issues
        ]
    }


def create_evaluation_error_result(error: Exception) -> SQLEvaluationResult:
    """
    Create an evaluation result for LLM invocation errors.
    
    Args:
        error: Exception that occurred during evaluation
        
    Returns:
        SQLEvaluationResult with error information
    """
    return SQLEvaluationResult(
        is_compliant=False,
        compliance_score=0.0,
        issues=[
            SQLIssue(
                issue_type="other",
                severity="error",
                description=f"Evaluation error: {str(error)}",
                suggestion="Check LLM connection and configuration"
            )
        ],
        syntax_valid=False,
        follows_best_practices=False,
        summary=f"Evaluation failed: {str(error)}"
    )


def create_error_issue_summary(
    sql_index: int,
    sql_statement: str,
    error: Exception,
    error_result: SQLEvaluationResult
) -> Dict[str, Any]:
    """
    Create an issue summary for evaluation errors.
    
    Args:
        sql_index: Index of the SQL statement
        sql_statement: The SQL statement that caused the error
        error: The exception that occurred
        error_result: The error evaluation result
        
    Returns:
        Dictionary containing error issue summary
    """
    return {
        "sql_index": sql_index,
        "sql_preview": clean_sql_preview(sql_statement),
        "error": str(error),
        "evaluation_result": error_result.model_dump()
    }


def should_skip_sql_statement(sql_statement: str) -> bool:
    """
    Check if a SQL statement should be skipped during evaluation.
    
    Args:
        sql_statement: SQL statement to check
        
    Returns:
        True if statement should be skipped
    """
    if not sql_statement:
        return True
    
    if sql_statement.strip().startswith("-- Error"):
        return True
    
    return False


def format_sql_statements_for_batch(sql_statements: List[str]) -> str:
    """
    Format multiple SQL statements for batch evaluation prompt.
    
    Args:
        sql_statements: List of SQL statements to format
        
    Returns:
        Formatted string with numbered SQL statements
    """
    formatted = []
    for idx, sql in enumerate(sql_statements, start=1):
        cleaned = sql.strip()
        if cleaned.startswith("```sql"):
            cleaned = cleaned[6:].strip()
        if cleaned.startswith("```"):
            cleaned = cleaned[3:].strip()
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].strip()
        
        formatted.append(f"SQL Statement {idx}:\n{cleaned}")
    
    return "\n\n---\n\n".join(formatted)


def evaluate_batch_sql_statements(
    sql_statements: List[str],
    statement_indices: List[int],
    structured_llm: Any,
    base_llm: Any
) -> List[Tuple[int, Optional[SQLEvaluationResult], Optional[Dict[str, Any]]]]:
    """
    Evaluate multiple SQL statements in a single LLM call.
    
    Args:
        sql_statements: List of SQL statements to evaluate
        statement_indices: List of original indices for each statement
        structured_llm: Structured output LLM instance configured for batch mode
        base_llm: Base LLM instance (fallback)
        
    Returns:
        List of tuples: (original_index, evaluation_result, issue_summary)
        Returns (index, None, None) if statement should be skipped
        Returns (index, result, None) if compliant
        Returns (index, result, issue_summary) if non-compliant
    """
    if not sql_statements:
        return []
    
    try:
        formatted_sql = format_sql_statements_for_batch(sql_statements)
        prompt = EvaluationPrompts.create_prompt(sql_content=formatted_sql)
        
        if structured_llm != base_llm:
            response = structured_llm.invoke(prompt)
        else:
            response = base_llm.invoke(prompt)
        
        batch_results = parse_batch_evaluation_response(response)
        
        if len(batch_results) != len(sql_statements):
            error_result = create_error_evaluation_result(
                f"Batch response has {len(batch_results)} results but expected {len(sql_statements)}"
            )
            return [
                (stmt_idx, error_result, create_error_issue_summary(stmt_idx, sql_stmt, Exception("Mismatched batch results"), error_result))
                for stmt_idx, sql_stmt in zip(statement_indices, sql_statements)
            ]
        
        results = []
        for stmt_idx, sql_stmt, eval_result in zip(statement_indices, sql_statements, batch_results):
            if eval_result is None:
                results.append((stmt_idx, None, None))
                continue
            
            if not eval_result.is_compliant:
                issue_summary = create_issue_summary(stmt_idx, sql_stmt, eval_result)
                results.append((stmt_idx, eval_result, issue_summary))
            else:
                results.append((stmt_idx, eval_result, None))
        
        return results
        
    except Exception as e:
        error_result = create_evaluation_error_result(e)
        return [
            (stmt_idx, error_result, create_error_issue_summary(stmt_idx, sql_stmt, e, error_result))
            for stmt_idx, sql_stmt in zip(statement_indices, sql_statements)
        ]


def evaluate_sql_compliance(
    batch: ArtifactBatch,
    translation_result: TranslationResult
) -> Tuple[bool, List[Dict[str, Any]], List[SQLEvaluationResult], List[int]]:
    """
    Evaluate SQL compliance using a fast/cheap LLM with structured output.
    Batches multiple SQL statements into single LLM calls for efficiency.

    Args:
        batch: The original artifact batch
        translation_result: The translation result containing SQL statements

    Returns:
        Tuple of (is_compliant: bool, issues: List[Dict], evaluation_results: List[SQLEvaluationResult], failed_indices: List[int])
    """
    llm = create_llm_for_node("evaluator")
    structured_llm = create_structured_llm(llm, batch_mode=True)
    batch_size = get_evaluation_batch_size()

    issues = []
    failed_evaluation_results = []
    failed_indices = []
    all_compliant = True

    sql_statements = []
    statement_indices = []
    
    for idx, sql_statement in enumerate(translation_result.results):
        if should_skip_sql_statement(sql_statement):
            continue
        
        sql_statements.append(sql_statement)
        statement_indices.append(idx)
        
        if len(sql_statements) >= batch_size:
            batch_results = evaluate_batch_sql_statements(sql_statements, statement_indices, structured_llm, llm)
            
            for stmt_idx, eval_result, issue_summary in batch_results:
                if eval_result is None:
                    continue
                
                if not eval_result.is_compliant:
                    all_compliant = False
                    failed_indices.append(stmt_idx)
                    failed_evaluation_results.append(eval_result)
                    
                    if issue_summary:
                        issues.append(issue_summary)
            
            sql_statements = []
            statement_indices = []
    
    if sql_statements:
        batch_results = evaluate_batch_sql_statements(sql_statements, statement_indices, structured_llm, llm)
        
        for stmt_idx, eval_result, issue_summary in batch_results:
            if eval_result is None:
                continue
            
            if not eval_result.is_compliant:
                all_compliant = False
                failed_indices.append(stmt_idx)
                failed_evaluation_results.append(eval_result)
                
                if issue_summary:
                    issues.append(issue_summary)

    return all_compliant, issues, failed_evaluation_results, failed_indices


def filter_failed_items(
    items: List[str],
    failed_indices: List[int]
) -> List[str]:
    """
    Filter items to only include those at failed indices.
    
    Args:
        items: List of items to filter
        failed_indices: List of indices to include
        
    Returns:
        Filtered list of items
    """
    return [items[idx] for idx in failed_indices if idx < len(items)]


def build_evaluation_result_data(
    batch: ArtifactBatch,
    translation_result: TranslationResult,
    issues: List[Dict[str, Any]],
    evaluation_results: List[SQLEvaluationResult],
    failed_indices: List[int],
    timestamp: str
) -> Dict[str, Any]:
    """
    Build the data structure for persisted failed batch.
    
    Args:
        batch: Original artifact batch
        translation_result: Translation result
        issues: List of issue summaries
        evaluation_results: List of failed evaluation results
        failed_indices: List of failed indices
        timestamp: Timestamp string
        
    Returns:
        Dictionary ready for JSON serialization
    """
    failed_items = filter_failed_items(batch.items, failed_indices)
    failed_sql_results = filter_failed_items(translation_result.results, failed_indices)
    failed_errors = filter_failed_items(translation_result.errors, failed_indices) if translation_result.errors else []
    
    average_score = (
        sum(r.compliance_score for r in evaluation_results) / len(evaluation_results)
        if evaluation_results else 0.0
    )
    
    return {
        "batch": {
            "artifact_type": batch.artifact_type,
            "items": failed_items,
            "context": batch.context,
            "original_batch_size": len(batch.items),
            "failed_count": len(failed_items)
        },
        "translation_result": {
            "artifact_type": translation_result.artifact_type,
            "results": failed_sql_results,
            "errors": failed_errors,
            "metadata": {
                **translation_result.metadata,
                "original_count": translation_result.metadata.get("count", len(translation_result.results)),
                "failed_count": len(failed_sql_results)
            }
        },
        "evaluation": {
            "issues": issues,
            "evaluation_results": [result.model_dump() for result in evaluation_results],
            "total_statements_evaluated": len(evaluation_results),
            "failed_statements": len(evaluation_results),
            "average_compliance_score": average_score
        },
        "timestamp": timestamp
    }


def get_evaluation_results_directory(batch_context: dict = None) -> str:
    """
    Get the directory path for storing evaluation results.
    
    Args:
        batch_context: Optional batch context to determine output directory
    
    Returns:
        Path to evaluation results directory
    """
    if batch_context and "results_dir" in batch_context:
        results_dir = batch_context["results_dir"]
        evaluation_results_dir = os.path.join(results_dir, "evaluation_results")
        os.makedirs(evaluation_results_dir, exist_ok=True)
        logger.debug("Using batch results_dir for evaluation outputs", extra={"results_dir": results_dir, "source_file": batch_context.get("source_file")})
        return evaluation_results_dir
    
    output_dir = os.getenv("DDL_OUTPUT_DIR", "./ddl_output")
    evaluation_results_dir = os.path.join(output_dir, "evaluation_results")
    os.makedirs(evaluation_results_dir, exist_ok=True)
    logger.warning("Batch context missing results_dir; falling back to global DDL_OUTPUT_DIR", extra={"fallback_dir": evaluation_results_dir, "batch_context_keys": list(batch_context.keys()) if batch_context else None})
    return evaluation_results_dir


def persist_evaluation_result(
    batch: ArtifactBatch, 
    translation_result: TranslationResult, 
    issues: List[Dict[str, Any]],
    evaluation_results: List[SQLEvaluationResult],
    failed_indices: List[int]
) -> str:
    """
    Persist evaluation result to a file with structured evaluation results.
    Only includes SQL statements that failed evaluation (non-compliant).

    Args:
        batch: The evaluated artifact batch
        translation_result: The translation result
        issues: List of structured compliance issues
        evaluation_results: List of structured evaluation results (only failed ones)
        failed_indices: List of indices of failed SQL statements

    Returns:
        Path to the persisted file
    """
    evaluation_results_dir = get_evaluation_results_directory(batch.context)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"evaluation_result_{batch.artifact_type}_{timestamp}.json"
    filepath = os.path.join(evaluation_results_dir, filename)

    evaluation_result_data = build_evaluation_result_data(
        batch, translation_result, issues, evaluation_results, failed_indices, timestamp
    )

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(evaluation_result_data, f, indent=2, default=str)

    return filepath


def evaluate_batch(
    batch: ArtifactBatch,
    translation_result: TranslationResult
) -> Tuple[bool, str, List[SQLEvaluationResult]]:
    """
    Evaluate a batch's translation result for SQL compliance using structured output.

    Args:
        batch: The artifact batch
        translation_result: The translation result to evaluate

    Returns:
        Tuple of (is_compliant: bool, persisted_file_path: str or empty string, failed_evaluation_results: List[SQLEvaluationResult])
    """
    is_compliant, issues, failed_evaluation_results, failed_indices = evaluate_sql_compliance(
        batch, translation_result
    )

    if not is_compliant:
        filepath = persist_evaluation_result(
            batch, translation_result, issues, failed_evaluation_results, failed_indices
        )
        return False, filepath, failed_evaluation_results

    return True, "", []
