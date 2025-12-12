from typing import List, Optional
from pydantic import BaseModel, Field


class SQLSyntaxValidationResult(BaseModel):
    """Simple result of SQL syntax validation for a single statement."""
    sql_statement: str = Field(
        description="The SQL statement being validated (cleaned, without markdown)"
    )
    syntax_valid: bool = Field(
        description="Whether the SQL syntax is valid for Databricks"
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error message if syntax is invalid, None if valid"
    )


class BatchSyntaxValidationResult(BaseModel):
    """Result of syntax validation for an entire batch."""
    results: List[SQLSyntaxValidationResult] = Field(
        description="List of syntax validation results, one per SQL statement in order"
    )
    total_statements: int = Field(
        description="Total number of SQL statements evaluated"
    )
    valid_statements: int = Field(
        description="Number of statements with valid syntax"
    )
    invalid_statements: int = Field(
        description="Number of statements with invalid syntax"
    )


class SQLIssue(BaseModel):
    """Represents a syntax error found during SQL evaluation."""
    description: str = Field(
        description="Description of the syntax error"
    )
    line_number: Optional[int] = Field(
        default=None,
        description="Line number where the error occurs (optional)"
    )
    suggestion: Optional[str] = Field(
        default=None,
        description="Suggestion for fixing the error (optional)"
    )


class SQLEvaluationResult(BaseModel):
    """Result of LLM-based SQL syntax evaluation for a single statement."""
    syntax_valid: bool = Field(
        description="Whether the SQL syntax is valid for Databricks"
    )
    error_message: Optional[str] = Field(
        default=None,
        description="Error message if syntax is invalid, None if valid"
    )
    issues: List[SQLIssue] = Field(
        default_factory=list,
        description="List of syntax errors found during evaluation"
    )


class BatchSQLEvaluationResult(BaseModel):
    """Result of LLM-based SQL evaluation for a batch of statements."""
    results: List[SQLEvaluationResult] = Field(
        description="List of evaluation results, one per SQL statement in order"
    )


class RouterResponse(BaseModel):
    """Structured response from the artifact router."""
    artifact_type: str = Field(
        description="The type of artifact detected. Must be one of: databases, schemas, tables, views, stages, external_locations, streams, pipes, roles, grants, tags, comments, masking_policies, udfs, procedures, file_formats"
    )
    confidence: Optional[str] = Field(
        default=None,
        description="Confidence level in the routing decision (optional)"
    )
    reasoning: Optional[str] = Field(
        default=None,
        description="Brief explanation of why this artifact type was chosen (optional)"
    )


