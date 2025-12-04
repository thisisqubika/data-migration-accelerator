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


