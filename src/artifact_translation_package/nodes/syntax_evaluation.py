import os
import json
import re
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional

import sqlglot
from artifact_translation_package.utils.types import ArtifactBatch, TranslationResult
from artifact_translation_package.utils.evaluation_models import SQLSyntaxValidationResult, BatchSyntaxValidationResult
from artifact_translation_package.utils.llm_utils import create_llm_for_node
from artifact_translation_package.utils.llm_evaluation_utils import (
    create_structured_llm,
    get_evaluation_batch_size,
    evaluate_batch_sql_statements,
    should_skip_sql_statement as llm_should_skip
)
from artifact_translation_package.config.ddl_config import get_config

# Set up logger for this module
logger = logging.getLogger(__name__)


def get_validation_config() -> dict:
    """Get validation configuration from DDL config."""
    config = get_config()
    return config.get("validation", {})


def should_use_llm_validation(artifact_type: str) -> bool:
    """Check if an artifact type should use LLM validation instead of SQLGlot."""
    validation_config = get_validation_config()
    llm_validated_artifacts = validation_config.get("llm_validated_artifacts", ["procedures", "pipes"])
    return artifact_type in llm_validated_artifacts


def should_skip_artifact_validation(artifact_type: str) -> bool:
    """Check if an artifact type should be skipped for validation."""
    validation_config = get_validation_config()
    skip_artifacts = validation_config.get("skip_unsupported_artifacts", [])
    return artifact_type in skip_artifacts


def should_report_all_results() -> bool:
    """Check if we should report all validation results (not just failures)."""
    validation_config = get_validation_config()
    return validation_config.get("report_all_results", False)


def is_validation_enabled() -> bool:
    """Check if validation is enabled."""
    validation_config = get_validation_config()
    return validation_config.get("enabled", True)


def normalize_newlines(sql_statement: str) -> str:
    """
    Normalize escaped newline characters to actual newlines.
    
    Args:
        sql_statement: SQL statement with potentially escaped newlines
        
    Returns:
        SQL statement with normalized newlines
    """
    cleaned = sql_statement
    
    cleaned = cleaned.replace("\\n", "\n")
    cleaned = cleaned.replace("\\r", "\r")
    cleaned = cleaned.replace("\\t", "\t")
    
    return cleaned


def clean_error_message(error_message: str) -> str:
    """
    Clean error message by removing ANSI escape codes and normalizing whitespace.
    
    Args:
        error_message: Raw error message from SQL parser
        
    Returns:
        Cleaned error message string
    """
    if not error_message:
        return ""
    
    cleaned = error_message
    
    cleaned = re.sub(r'\x1b\[[0-9;]*m', '', cleaned)
    cleaned = cleaned.replace("\u001b[4m", "").replace("\u001b[0m", "")
    
    cleaned = cleaned.replace("\\n", "\n")
    cleaned = cleaned.replace("\\r", "\r")
    
    cleaned = re.sub(r'\n\s*\n', '\n', cleaned)
    
    return cleaned.strip()


def clean_sql_statement(sql_statement: str) -> str:
    """
    Clean SQL statement by removing markdown code blocks and normalizing newlines.
    
    Args:
        sql_statement: Original SQL statement
        
    Returns:
        Cleaned SQL statement with normalized newlines
    """
    cleaned = sql_statement.strip()
    
    if cleaned.startswith("```sql"):
        cleaned = cleaned[6:].strip()
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:].strip()
    
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3].strip()
    
    cleaned = normalize_newlines(cleaned)
    
    return cleaned


def clean_sql_preview(sql_statement: str, max_length: int = 200) -> str:
    """
    Clean SQL statement for preview display in JSON.
    
    Args:
        sql_statement: Original SQL statement
        max_length: Maximum length of preview
        
    Returns:
        Cleaned SQL preview string
    """
    cleaned = clean_sql_statement(sql_statement)
    cleaned = cleaned.replace("\n", " ").replace("\r", " ")
    cleaned = " ".join(cleaned.split())
    
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length] + "..."
    
    return cleaned


def is_procedure_or_function(sql_statement: str) -> bool:
    """
    Check if SQL statement is a CREATE PROCEDURE or CREATE FUNCTION statement.

    Args:
        sql_statement: SQL statement to check

    Returns:
        True if statement is a procedure or function definition
    """
    sql_upper = sql_statement.upper().strip()
    result = (sql_upper.startswith("CREATE PROCEDURE") or
            sql_upper.startswith("CREATE OR REPLACE PROCEDURE") or
            sql_upper.startswith("CREATE FUNCTION") or
            sql_upper.startswith("CREATE OR REPLACE FUNCTION"))

    if result:
        logger.debug(f"Detected procedure/function statement: {sql_statement[:100]}...")

    return result


def validate_sql_syntax(sql_statement: str, dialect: str = "databricks") -> Tuple[bool, Optional[str], Optional[List[Dict[str, Any]]]]:
    """
    Validate SQL syntax using SQLGlot parser.
    
    Skips validation for procedures and functions since SQLGlot doesn't fully support them.
    
    Args:
        sql_statement: SQL statement to validate
        dialect: Target SQL dialect (default: databricks)
        
    Returns:
        Tuple of (is_valid: bool, error_message: Optional[str], issues: Optional[List[Dict]])
    """
    cleaned_sql = clean_sql_statement(sql_statement)
    
    if not cleaned_sql or cleaned_sql.strip().startswith("-- Error"):
        logger.debug("Skipping validation: empty or error statement")
        return True, None, None
    
    try:
        logger.debug(f"Attempting to parse SQL with SQLGlot (dialect: {dialect})")
        parsed = sqlglot.parse_one(cleaned_sql, dialect=dialect)
        logger.debug(f"SQLGlot parse result: {parsed is not None}")

        if parsed is None:
            logger.warning("SQLGlot returned None - invalid syntax detected")
            return False, "Failed to parse SQL statement", [
                {
                    "type": "syntax_error",
                    "severity": "error",
                    "description": "SQL parser returned None - invalid syntax",
                    "suggestion": "Check SQL syntax against Databricks documentation"
                }
            ]

        try:
            logger.debug("Attempting to transpile SQL")
            sqlglot.transpile(cleaned_sql, read=dialect, write=dialect)
            logger.debug("SQL validation successful")
            return True, None, None
        except Exception as transpile_error:
            error_msg = clean_error_message(str(transpile_error))
            logger.warning(f"SQL transpilation failed: {error_msg}")
            return False, error_msg, [
                {
                    "type": "syntax_error",
                    "severity": "error",
                    "description": f"SQL validation failed: {error_msg}",
                    "suggestion": "Review SQL syntax and ensure compatibility with Databricks"
                }
            ]

    except sqlglot.errors.ParseError as e:
        error_msg = clean_error_message(str(e))
        line_number = None
        if hasattr(e, 'lineno'):
            line_number = e.lineno

        logger.warning(f"SQLGlot parse error: {error_msg} (line: {line_number})")
        return False, error_msg, [
            {
                "type": "syntax_error",
                "severity": "error",
                "description": f"Parse error: {error_msg}",
                "line_number": line_number,
                "suggestion": "Fix SQL syntax errors and try again"
            }
        ]
    except Exception as e:
        error_msg = clean_error_message(str(e))
        logger.error(f"Unexpected error during SQL validation: {error_msg}")
        return False, error_msg, [
            {
                "type": "syntax_error",
                "severity": "error",
                "description": f"Unexpected error during validation: {error_msg}",
                "suggestion": "Check SQL statement format"
            }
        ]


def evaluate_sql_with_syntax_validator(
    sql_statement: str
) -> SQLSyntaxValidationResult:
    """
    Evaluate SQL statement using syntax validator.
    Only checks syntax validity.
    
    Args:
        sql_statement: SQL statement to evaluate (will be cleaned)
        
    Returns:
        SQLSyntaxValidationResult with syntax validity status and cleaned SQL
    """
    cleaned_sql = clean_sql_statement(sql_statement)
    syntax_valid, error_message, _ = validate_sql_syntax(cleaned_sql)
    
    return SQLSyntaxValidationResult(
        sql_statement=cleaned_sql,
        syntax_valid=syntax_valid,
        error_message=error_message if not syntax_valid else None
    )


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


def _convert_llm_result_to_validation_result(
    sql_statement: str,
    eval_result: Optional[Any]
) -> SQLSyntaxValidationResult:
    """
    Convert LLM evaluation result to syntax validation result.

    Args:
        sql_statement: Original SQL statement
        eval_result: LLM evaluation result or None

    Returns:
        SQLSyntaxValidationResult
    """
    cleaned_sql = clean_sql_statement(sql_statement)
    
    if eval_result is None:
        return SQLSyntaxValidationResult(
            sql_statement=cleaned_sql,
            syntax_valid=True,
            error_message=None
        )
    
    return SQLSyntaxValidationResult(
        sql_statement=cleaned_sql,
        syntax_valid=eval_result.syntax_valid,
        error_message=eval_result.error_message
    )


def _process_llm_batch_results(
    batch_results: List[Tuple[int, Optional[Any], Optional[Dict[str, Any]]]],
    translation_result: TranslationResult
) -> List[SQLSyntaxValidationResult]:
    """
    Process LLM batch evaluation results into validation results.

    Args:
        batch_results: List of (index, eval_result, issue_summary) tuples
        translation_result: Translation result containing SQL statements

    Returns:
        List of SQLSyntaxValidationResult objects
    """
    validation_results = []
    for stmt_idx, eval_result, _ in batch_results:
        validation_result = _convert_llm_result_to_validation_result(
            translation_result.results[stmt_idx],
            eval_result
        )
        validation_results.append(validation_result)
    return validation_results


def evaluate_sql_compliance_with_llm(
    batch: ArtifactBatch,
    translation_result: TranslationResult
) -> Tuple[BatchSyntaxValidationResult, List[int]]:
    """
    Evaluate SQL syntax validity using LLM validation.

    Args:
        batch: The original artifact batch
        translation_result: The translation result containing SQL statements

    Returns:
        Tuple of (BatchSyntaxValidationResult, evaluated_indices: List[int])
    """
    logger.debug(f"Evaluating SQL syntax with LLM for {len(translation_result.results)} statements")

    llm = create_llm_for_node("evaluator")
    structured_llm = create_structured_llm(llm, batch_mode=True)
    batch_size = get_evaluation_batch_size()

    evaluated_indices = []
    validation_results = []
    sql_statements = []
    statement_indices = []
    
    for idx, sql_statement in enumerate(translation_result.results):
        if llm_should_skip(sql_statement):
            logger.debug(f"Skipping statement {idx}: marked for skip")
            continue
        
        evaluated_indices.append(idx)
        sql_statements.append(sql_statement)
        statement_indices.append(idx)
        
        if len(sql_statements) >= batch_size:
            batch_results = evaluate_batch_sql_statements(sql_statements, statement_indices, structured_llm, llm)
            validation_results.extend(_process_llm_batch_results(batch_results, translation_result))
            sql_statements = []
            statement_indices = []
    
    if sql_statements:
        batch_results = evaluate_batch_sql_statements(sql_statements, statement_indices, structured_llm, llm)
        validation_results.extend(_process_llm_batch_results(batch_results, translation_result))
    
    valid_count = sum(1 for r in validation_results if r.syntax_valid)
    invalid_count = len(validation_results) - valid_count
    
    batch_result = BatchSyntaxValidationResult(
        results=validation_results,
        total_statements=len(validation_results),
        valid_statements=valid_count,
        invalid_statements=invalid_count
    )
    
    logger.info(f"LLM syntax evaluation complete: {invalid_count}/{len(validation_results)} statements have invalid syntax")
    return batch_result, evaluated_indices


def evaluate_sql_compliance(
    batch: ArtifactBatch,
    translation_result: TranslationResult
) -> Tuple[BatchSyntaxValidationResult, List[int]]:
    """
    Evaluate SQL syntax validity for all statements in a batch.

    Args:
        batch: The original artifact batch
        translation_result: The translation result containing SQL statements

    Returns:
        Tuple of (BatchSyntaxValidationResult, evaluated_indices: List[int])
    """
    logger.debug(f"Evaluating SQL syntax for {len(translation_result.results)} statements")

    validation_results = []
    evaluated_indices = []

    for idx, sql_statement in enumerate(translation_result.results):
        if should_skip_sql_statement(sql_statement):
            logger.debug(f"Skipping statement {idx}: marked for skip")
            continue

        evaluated_indices.append(idx)
        validation_result = evaluate_sql_with_syntax_validator(sql_statement)
        validation_results.append(validation_result)

        if not validation_result.syntax_valid:
            logger.warning(f"Statement {idx} has invalid syntax: {validation_result.error_message}")
        else:
            logger.debug(f"Statement {idx} has valid syntax")

    valid_count = sum(1 for r in validation_results if r.syntax_valid)
    invalid_count = len(validation_results) - valid_count

    batch_result = BatchSyntaxValidationResult(
        results=validation_results,
        total_statements=len(validation_results),
        valid_statements=valid_count,
        invalid_statements=invalid_count
    )

    logger.info(f"SQL syntax evaluation complete: {invalid_count}/{len(validation_results)} statements have invalid syntax")
    return batch_result, evaluated_indices


def filter_items_by_indices(
    items: List[str],
    indices: List[int]
) -> List[str]:
    """
    Filter items to only include those at specified indices.

    Args:
        items: List of items to filter
        indices: List of indices to include

    Returns:
        Filtered list of items
    """
    return [items[idx] for idx in indices if idx < len(items)]


def build_evaluation_batch_data(
    batch: ArtifactBatch,
    translation_result: TranslationResult,
    validation_result: BatchSyntaxValidationResult,
    evaluated_indices: List[int],
    timestamp: str
) -> Dict[str, Any]:
    """
    Build the data structure for persisted evaluation batch.

    Args:
        batch: Original artifact batch
        translation_result: Translation result
        validation_result: Batch syntax validation result
        evaluated_indices: List of evaluated indices
        timestamp: Timestamp string

    Returns:
        Dictionary ready for JSON serialization
    """
    evaluated_items = filter_items_by_indices(batch.items, evaluated_indices)
    evaluated_sql_results_raw = filter_items_by_indices(translation_result.results, evaluated_indices)
    evaluated_sql_results_cleaned = [clean_sql_statement(sql) for sql in evaluated_sql_results_raw]
    evaluated_errors = filter_items_by_indices(translation_result.errors, evaluated_indices) if translation_result.errors else []

    return {
        "batch": {
            "artifact_type": batch.artifact_type,
            "items": evaluated_items,
            "context": batch.context,
            "original_batch_size": len(batch.items),
            "evaluated_count": len(evaluated_items)
        },
        "translation_result": {
            "artifact_type": translation_result.artifact_type,
            "results": evaluated_sql_results_cleaned,
            "errors": evaluated_errors,
            "metadata": {
                **translation_result.metadata,
                "original_count": translation_result.metadata.get("count", len(translation_result.results)),
                "evaluated_count": len(evaluated_sql_results_cleaned)
            }
        },
        "validation": {
            "results": [result.model_dump() for result in validation_result.results],
            "total_statements": validation_result.total_statements,
            "valid_statements": validation_result.valid_statements,
            "invalid_statements": validation_result.invalid_statements,
            "validation_method": "llm_validator" if should_use_llm_validation(batch.artifact_type) else "syntax_validator"
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
    # If batch context has results_dir, use it (from main.py timestamped folder)
    if batch_context and "results_dir" in batch_context:
        results_dir = batch_context["results_dir"]
        evaluation_results_dir = os.path.join(results_dir, "evaluation_results")
        os.makedirs(evaluation_results_dir, exist_ok=True)
        return evaluation_results_dir
    
    # Fallback to environment variable or default
    output_dir = os.getenv("DDL_OUTPUT_DIR", "./ddl_output")
    evaluation_results_dir = os.path.join(output_dir, "evaluation_results")
    os.makedirs(evaluation_results_dir, exist_ok=True)
    return evaluation_results_dir


def persist_evaluation_batch(
    batch: ArtifactBatch,
    translation_result: TranslationResult,
    validation_result: BatchSyntaxValidationResult,
    evaluated_indices: List[int]
) -> str:
    """
    Persist an evaluation batch to a JSON file for analysis.

    Args:
        batch: The evaluated artifact batch
        translation_result: The translation result
        validation_result: Batch syntax validation result
        evaluated_indices: List of indices of evaluated SQL statements

    Returns:
        Path to the persisted file
    """
    logger.info(f"Persisting validation batch with {len(evaluated_indices)} evaluated statements")

    evaluation_results_dir = get_evaluation_results_directory(batch.context)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"evaluation_batch_{batch.artifact_type}_{timestamp}.json"
    filepath = os.path.join(evaluation_results_dir, filename)

    evaluation_batch_data = build_evaluation_batch_data(
        batch, translation_result, validation_result, evaluated_indices, timestamp
    )

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(evaluation_batch_data, f, indent=2, default=str)

    return filepath


def evaluate_batch(
    batch: ArtifactBatch,
    translation_result: TranslationResult
) -> Tuple[bool, str, BatchSyntaxValidationResult]:
    """
    Evaluate a batch's translation result for SQL syntax validity.

    Always provides validation results based on configuration.
    Skips unsupported artifacts unless validation is disabled.

    Args:
        batch: The artifact batch
        translation_result: The translation result to evaluate

    Returns:
        Tuple of (all_valid: bool, persisted_file_path: str or empty string, validation_result: BatchSyntaxValidationResult)
    """
    logger.info(f"Starting batch validation for artifact type: {batch.artifact_type}, batch size: {len(batch.items)}")

    # Check if validation is enabled
    if not is_validation_enabled():
        logger.info("Validation disabled via configuration")
        return True, "", BatchSyntaxValidationResult(
            results=[],
            total_statements=0,
            valid_statements=0,
            invalid_statements=0
        )

    # Check if this artifact type should be skipped
    if should_skip_artifact_validation(batch.artifact_type):
        logger.info(f"Skipping validation for {batch.artifact_type} - unsupported by SQLGlot")
        return True, "", BatchSyntaxValidationResult(
            results=[],
            total_statements=0,
            valid_statements=0,
            invalid_statements=0
        )

    logger.debug(f"Evaluating {len(translation_result.results)} translated SQL statements")

    if should_use_llm_validation(batch.artifact_type):
        logger.info(f"Using LLM validation for {batch.artifact_type}")
        validation_result, evaluated_indices = evaluate_sql_compliance_with_llm(
            batch, translation_result
        )
    else:
        logger.debug(f"Using SQLGlot validation for {batch.artifact_type}")
        validation_result, evaluated_indices = evaluate_sql_compliance(
            batch, translation_result
        )

    # Always persist validation results when statements were evaluated
    if evaluated_indices:
        filepath = persist_evaluation_batch(
            batch, translation_result, validation_result, evaluated_indices
        )
        logger.info(f"Persisted validation results to: {filepath}")
        all_valid = validation_result.invalid_statements == 0
        return all_valid, filepath, validation_result

    logger.info("No statements evaluated")
    return True, "", BatchSyntaxValidationResult(
        results=[],
        total_statements=0,
        valid_statements=0,
        invalid_statements=0
    )

