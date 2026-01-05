"""
Databricks SQL compliance evaluation prompt.
"""

DATABRICKS_COMPLIANCE_PROMPT = """You are an expert Databricks SQL architect.
Evaluate the provided SQL DDL in TWO INDEPENDENT DIMENSIONS:

**1. COMPLIANCE (0-100): Does it meet Databricks SQL requirements?**
Start at 100, deduct points for:
- Invalid syntax: -100 (score becomes 0)
- Missing Unity Catalog 3-level naming: -15
- Using VARCHAR/TEXT/CHAR instead of STRING: -10
- Using INT/INTEGER instead of BIGINT for IDs: -10
- Missing USING DELTA: -20
- MixedCase identifiers (should be snake_case): -10

**2. BEST PRACTICES (0-100): Is it optimized and documented?**
Start at 100, deduct points for:
- Missing CLUSTER BY (Liquid Clustering): -30
- Missing table properties (autoOptimize, etc): -20
- Missing table COMMENT: -25
- Missing column COMMENTs: -25

**SQL to Evaluate:**
{sql_content}

**Instructions:**
- Evaluate BOTH dimensions independently
- Compliance = functional correctness for Databricks
- Best Practices = production-ready optimizations and documentation
- A SQL can be 100% compliant but 0% best practices (runs but unoptimized)

Return JSON:
{{
    "results": [
        {{
            "syntax_valid": boolean,
            "score": integer (0-100, compliance),
            "best_practices_score": integer (0-100),
            "error_message": string or null,
            "issues": [
                {{
                    "category": "compliance" | "best_practices",
                    "description": string,
                    "suggestion": string,
                    "severity": "critical" | "warning" | "info"
                }}
            ]
        }}
    ]
}}
"""


def create_compliance_prompt(sql_content: str) -> str:
    """Create a Databricks SQL compliance evaluation prompt."""
    return DATABRICKS_COMPLIANCE_PROMPT.format(sql_content=sql_content)
