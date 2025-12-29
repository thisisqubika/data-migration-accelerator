"""
Utility functions for LLM-based SQL evaluation.

These functions are used for evaluating SQL syntax using LLM instead of SQLGlot,
particularly for complex artifacts like procedures and pipes.
"""

from typing import List, Tuple, Dict, Any, Optional

try:
    from langchain_core.output_parsers import PydanticOutputParser
except ImportError:
    PydanticOutputParser = None

from artifact_translation_package.utils.evaluation_models import SQLEvaluationResult, SQLIssue, BatchSQLEvaluationResult
from artifact_translation_package.prompts.evaluation_prompts import EvaluationPrompts
from artifact_translation_package.config.ddl_config import get_config
from artifact_translation_package.utils.sql_cleaner import clean_sql_preview


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
    from artifact_translation_package.utils.sql_cleaner import remove_markdown_code_blocks
    
    formatted = []
    for idx, sql in enumerate(sql_statements, start=1):
        cleaned = remove_markdown_code_blocks(sql.strip())
        formatted.append(f"SQL Statement {idx}:\n{cleaned}")
    
    return "\n\n---\n\n".join(formatted)


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



def create_error_evaluation_result(error_message: str) -> SQLEvaluationResult:
    """
    Create an error evaluation result for evaluation failures.
    
    Args:
        error_message: Error message describing the failure
        
    Returns:
        SQLEvaluationResult with error information
    """
    return SQLEvaluationResult(
        syntax_valid=False,
        error_message=f"Evaluation failed: {error_message}",
        issues=[
            SQLIssue(
                description=f"Evaluation failed: {error_message}",
                suggestion="Check LLM response and configuration"
            )
        ]
    )


def create_evaluation_error_result(error: Exception) -> SQLEvaluationResult:
    """
    Create an evaluation result for LLM invocation errors.
    
    Args:
        error: Exception that occurred during evaluation
        
    Returns:
        SQLEvaluationResult with error information
    """
    return SQLEvaluationResult(
        syntax_valid=False,
        error_message=f"Evaluation failed: {str(error)}",
        issues=[
            SQLIssue(
                description=f"Evaluation error: {str(error)}",
                suggestion="Check LLM connection and configuration"
            )
        ]
    )


def create_issue_summary(
    sql_index: int,
    sql_statement: str,
    evaluation_result: SQLEvaluationResult
) -> Dict[str, Any]:
    """
    Create a summary dictionary for a SQL statement with syntax errors.
    
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
        "syntax_valid": evaluation_result.syntax_valid,
        "error_message": evaluation_result.error_message,
        "issues": [
            {
                "description": issue.description,
                "line_number": issue.line_number,
                "suggestion": issue.suggestion
            }
            for issue in evaluation_result.issues
        ]
    }


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
            
            if not eval_result.syntax_valid:
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
