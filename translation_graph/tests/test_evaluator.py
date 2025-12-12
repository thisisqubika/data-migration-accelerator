#!/usr/bin/env python3
"""
Test script for SQL evaluator with bad syntax inputs.

Tests the evaluation node with various SQL statements containing:
- Syntax errors
- Best practice violations
- Invalid Databricks SQL constructs
- Valid SQL for comparison
"""

import sys
import os
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nodes.evaluation import evaluate_batch
from utils.types import ArtifactBatch, TranslationResult


def get_test_sql_statements() -> List[Dict[str, str]]:
    """
    Get a collection of test SQL statements with various issues.
    
    Returns:
        List of dictionaries with 'name' and 'sql' keys
    """
    return [
        {
            "name": "Valid SQL - Simple Table",
            "sql": "CREATE TABLE IF NOT EXISTS my_catalog.my_schema.my_table (\n  id BIGINT NOT NULL,\n  name STRING,\n  created_at TIMESTAMP\n) USING DELTA;"
        },
        {
            "name": "Syntax Error - Invalid Function Call",
            "sql": "CREATE TABLE test_table (\n  id BIGINT NOT NULL,\n  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()\n);"
        },
        {
            "name": "Syntax Error - Snowflake Syntax",
            "sql": "CREATE TABLE test_table (\n  id NUMBER(38,0),\n  name VARCHAR(255),\n  created_at TIMESTAMP_NTZ\n);"
        },
        {
            "name": "Best Practice Violation - Missing Partitioning",
            "sql": "CREATE TABLE large_table (\n  id BIGINT,\n  event_date DATE,\n  data STRING\n) USING DELTA;"
        },
        {
            "name": "Syntax Error - Invalid Data Type",
            "sql": "CREATE TABLE test_table (\n  id INT,\n  name TEXT,\n  amount DECIMAL(10,2)\n);"
        },
        {
            "name": "Best Practice Violation - Poor Naming",
            "sql": "CREATE TABLE tbl1 (\n  col1 BIGINT,\n  col2 STRING\n);"
        },
        {
            "name": "Syntax Error - Invalid Catalog Reference",
            "sql": "CREATE TABLE database.schema.table (\n  id BIGINT\n);"
        },
        {
            "name": "Valid SQL - Properly Formatted",
            "sql": "CREATE TABLE IF NOT EXISTS my_catalog.my_schema.users (\n  user_id BIGINT NOT NULL COMMENT 'User identifier',\n  username STRING NOT NULL COMMENT 'Username',\n  email STRING COMMENT 'Email address',\n  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Creation timestamp'\n) USING DELTA\nPARTITIONED BY (created_at)\nCOMMENT 'User accounts table';"
        },
        {
            "name": "Syntax Error - Missing Required Keywords",
            "sql": "CREATE TABLE test (\n  id BIGINT\n);"
        },
        {
            "name": "Best Practice Violation - No Comments",
            "sql": "CREATE TABLE products (\n  product_id BIGINT,\n  product_name STRING,\n  price DECIMAL(10,2)\n) USING DELTA;"
        }
    ]


def create_test_batch(sql_statements: List[str]) -> ArtifactBatch:
    """
    Create a test ArtifactBatch from SQL statements.
    
    Args:
        sql_statements: List of SQL statement strings
        
    Returns:
        ArtifactBatch object
    """
    import json
    items = [json.dumps({"sql": sql}) for sql in sql_statements]
    return ArtifactBatch(
        artifact_type="tables",
        items=items,
        context={
            "source_db": "snowflake",
            "target_db": "databricks",
            "test_mode": True
        }
    )


def create_test_translation_result(sql_statements: List[str]) -> TranslationResult:
    """
    Create a test TranslationResult from SQL statements.
    
    Args:
        sql_statements: List of SQL statement strings
        
    Returns:
        TranslationResult object
    """
    return TranslationResult(
        artifact_type="tables",
        results=sql_statements,
        errors=[],
        metadata={
            "count": len(sql_statements),
            "processed": len(sql_statements)
        }
    )


def print_evaluation_results(
    test_cases: List[Dict[str, str]],
    is_compliant: bool,
    issues: List[Dict[str, Any]],
    failed_evaluation_results: List[Any]
) -> None:
    """
    Print evaluation results in a readable format.
    
    Args:
        test_cases: List of test cases with names and SQL
        is_compliant: Overall compliance status
        issues: List of issue summaries
        failed_evaluation_results: List of failed evaluation results
    """
    print("=" * 80)
    print("SQL EVALUATOR TEST RESULTS")
    print("=" * 80)
    print(f"\nOverall Compliance: {'✅ COMPLIANT' if is_compliant else '❌ NON-COMPLIANT'}")
    print(f"Total Statements: {len(test_cases)}")
    print(f"Failed Statements: {len(issues)}")
    print(f"Compliant Statements: {len(test_cases) - len(issues)}")
    print("\n" + "=" * 80)
    
    issue_map = {issue["sql_index"]: issue for issue in issues}
    
    for idx, test_case in enumerate(test_cases):
        print(f"\n[{idx + 1}] {test_case['name']}")
        print("-" * 80)
        print(f"SQL Preview: {test_case['sql'][:100]}...")
        
        if idx in issue_map:
            issue = issue_map[idx]
            print(f"Status: ❌ NON-COMPLIANT")
            print(f"Compliance Score: {issue.get('compliance_score', 'N/A')}")
            print(f"Syntax Valid: {issue.get('syntax_valid', 'N/A')}")
            print(f"Follows Best Practices: {issue.get('follows_best_practices', 'N/A')}")
            print(f"Summary: {issue.get('summary', 'N/A')}")
            
            if issue.get('issues'):
                print("\nIssues Found:")
                for i, sql_issue in enumerate(issue['issues'], 1):
                    print(f"  {i}. [{sql_issue.get('severity', 'unknown').upper()}] {sql_issue.get('type', 'unknown')}")
                    print(f"     Description: {sql_issue.get('description', 'N/A')}")
                    if sql_issue.get('suggestion'):
                        print(f"     Suggestion: {sql_issue.get('suggestion', 'N/A')}")
                    if sql_issue.get('line_number'):
                        print(f"     Line: {sql_issue.get('line_number')}")
        else:
            print("Status: ✅ COMPLIANT")
        
        print()


def main():
    """Main test function."""
    print("Starting SQL Evaluator Test with Bad Syntax Inputs")
    print("=" * 80)
    print()
    
    test_cases = get_test_sql_statements()
    sql_statements = [case["sql"] for case in test_cases]
    
    batch = create_test_batch(sql_statements)
    translation_result = create_test_translation_result(sql_statements)
    
    print(f"Testing {len(sql_statements)} SQL statements...")
    print(f"Evaluation batch size: 5 statements per LLM call")
    print()
    
    try:
        is_compliant, persisted_file, failed_evaluation_results = evaluate_batch(
            batch, translation_result
        )
        
        issues = []
        if not is_compliant and persisted_file:
            import json
            with open(persisted_file, 'r') as f:
                persisted_data = json.load(f)
                issues = persisted_data.get("evaluation", {}).get("issues", [])
        
        print_evaluation_results(test_cases, is_compliant, issues, failed_evaluation_results)
        
        if persisted_file:
            print("\n" + "=" * 80)
            print(f"Failed batch persisted to: {persisted_file}")
            print("=" * 80)
        
        print("\n✅ Test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

