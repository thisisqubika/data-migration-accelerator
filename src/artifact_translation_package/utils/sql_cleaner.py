"""
SQL cleaning utilities for the Migration Accelerator.

This module provides centralized SQL statement cleaning functions to ensure
consistent behavior across the codebase. It handles markdown code block removal,
newline normalization, and SQL preview generation.
"""

import re
from typing import Optional


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


def remove_markdown_code_blocks(sql_statement: str) -> str:
    """
    Remove markdown code block markers from SQL statement.
    
    Handles both ```sql and ``` markers at the beginning and end.
    
    Args:
        sql_statement: SQL statement potentially wrapped in markdown code blocks
        
    Returns:
        SQL statement without markdown code block markers
    """
    cleaned = sql_statement.strip()
    
    # Remove opening code block markers
    if cleaned.startswith("```sql"):
        cleaned = cleaned[6:].strip()
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:].strip()
    
    # Remove closing code block markers
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3].strip()
    
    return cleaned


def clean_sql_statement(sql_statement: str) -> str:
    """
    Clean SQL statement by removing markdown code blocks and normalizing newlines.
    
    This is the main cleaning function that combines all cleaning operations.
    
    Args:
        sql_statement: Original SQL statement
        
    Returns:
        Cleaned SQL statement with normalized newlines and no markdown markers
    """
    cleaned = sql_statement.strip()
    cleaned = remove_markdown_code_blocks(cleaned)
    cleaned = normalize_newlines(cleaned)
    return cleaned


def clean_sql_preview(sql_statement: str, max_length: int = 200) -> str:
    """
    Clean SQL statement for preview display in JSON or logs.
    
    Removes markdown code blocks, normalizes whitespace, and truncates
    to specified maximum length.
    
    Args:
        sql_statement: Original SQL statement
        max_length: Maximum length of preview (default: 200)
        
    Returns:
        Cleaned SQL preview string suitable for display
    """
    cleaned = sql_statement.strip()
    cleaned = remove_markdown_code_blocks(cleaned)
    
    # Replace newlines with spaces and normalize whitespace
    cleaned = cleaned.replace("\n", " ").replace("\r", " ")
    cleaned = " ".join(cleaned.split())
    
    # Truncate if necessary
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length] + "..."
    
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
    
    # Remove ANSI escape codes
    cleaned = re.sub(r'\x1b\[[0-9;]*m', '', cleaned)
    cleaned = cleaned.replace("\u001b[4m", "").replace("\u001b[0m", "")
    
    # Normalize newlines
    cleaned = cleaned.replace("\\n", "\n").replace("\\r", "\r")
    
    # Remove excessive blank lines
    cleaned = re.sub(r'\n\s*\n', '\n', cleaned)
    
    return cleaned.strip()


def remove_trailing_semicolons(sql_statement: str) -> str:
    """
    Remove trailing semicolons from SQL statement.
    
    This is useful when appending semicolons to avoid double ';;'.
    
    Args:
        sql_statement: SQL statement potentially ending with semicolons
        
    Returns:
        SQL statement without trailing semicolons
    """
    cleaned = sql_statement.rstrip()
    while cleaned.endswith(';'):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


def format_sql_for_file(sql_statement: str, statement_number: int) -> str:
    """
    Format SQL statement for writing to a file.
    
    Adds comment header and ensures proper semicolon termination.
    
    Args:
        sql_statement: SQL statement to format
        statement_number: Statement number for the comment header
        
    Returns:
        Formatted SQL string ready for file writing
    """
    cleaned = clean_sql_statement(sql_statement)
    cleaned = remove_trailing_semicolons(cleaned)
    
    formatted = f"-- Statement {statement_number}\n"
    formatted += cleaned
    formatted += ";\n\n"
    
    return formatted