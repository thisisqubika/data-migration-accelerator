"""
Databricks SQL compliance scorer for MLflow evaluation.

Provides an LLM-as-a-judge scorer for evaluating Databricks SQL compliance.
"""

import logging
from typing import Dict, Any, Optional

from databricks_langchain import ChatDatabricks

from artifact_translation_package.config.ddl_config import create_node_llm
from artifact_translation_package.nodes.evaluation import create_structured_llm
from artifact_translation_package.evaluation.compliance_prompts import create_compliance_prompt
from artifact_translation_package.utils.evaluation_models import SQLEvaluationResult

logger = logging.getLogger(__name__)

COMPLIANT_THRESHOLD = 70
DEFAULT_VALID_SCORE = 80.0


def _clean_sql(sql: str) -> str:
    """Remove markdown code fences from SQL."""
    cleaned = sql.strip()
    if cleaned.startswith("```sql"):
        cleaned = cleaned[6:].strip()
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:].strip()
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3].strip()
    return cleaned


def _build_metrics(eval_result: SQLEvaluationResult) -> Dict[str, Any]:
    """Convert evaluation result to metrics dictionary."""
    compliance_score = float(getattr(eval_result, "score", 0))
    best_practices_score = float(getattr(eval_result, "best_practices_score", 0))
    
    if compliance_score == 0 and eval_result.syntax_valid:
        compliance_score = DEFAULT_VALID_SCORE
    
    return {
        "is_compliant": compliance_score >= COMPLIANT_THRESHOLD,
        "compliance_score": compliance_score,
        "best_practices_score": best_practices_score,
        "syntax_valid": eval_result.syntax_valid,
        "summary": eval_result.error_message or f"Compliance: {compliance_score}, Best Practices: {best_practices_score}",
        "issues_count": len(eval_result.issues),
        "issues": [
            {
                "type": getattr(issue, "category", "syntax"),
                "severity": getattr(issue, "severity", "warning"),
                "description": issue.description,
                "suggestion": issue.suggestion or "",
                "line_number": issue.line_number
            }
            for issue in eval_result.issues
        ]
    }


class DatabricksSQLComplianceScorer:
    """LLM-as-judge scorer for Databricks SQL compliance."""
    
    def __init__(self, llm_endpoint: Optional[str] = None):
        self.llm_endpoint = llm_endpoint
        self._structured_llm = None
        
    def _init_llm(self):
        """Initialize structured LLM on first use."""
        if self._structured_llm is not None:
            return
        
        if self.llm_endpoint:
            llm = ChatDatabricks(endpoint=self.llm_endpoint, temperature=0.1, max_tokens=2000)
        else:
            llm = create_node_llm("evaluator")
        
        self._structured_llm = create_structured_llm(llm, batch_mode=True)
    
    def evaluate_sql(self, sql_statement: str) -> Dict[str, Any]:
        """Evaluate SQL statement for Databricks compliance."""
        if not sql_statement or not sql_statement.strip():
            return {
                "is_compliant": False,
                "compliance_score": 0.0,
                "syntax_valid": False,
                "summary": "Empty SQL",
                "error": "No content"
            }
        
        try:
            self._init_llm()
            prompt = create_compliance_prompt(sql_content=_clean_sql(sql_statement))
            response = self._structured_llm.invoke(prompt)
            # Structured output always returns BatchSQLEvaluationResult with .results list
            eval_result = response.results[0]
            return _build_metrics(eval_result)
            
        except Exception as e:
            logger.error(f"Evaluation error: {e}")
            return {
                "is_compliant": False,
                "compliance_score": 0.0,
                "syntax_valid": False,
                "summary": f"Error: {e}",
                "error": str(e)
            }
    
    def __call__(self, sql_statement: str) -> Dict[str, Any]:
        return self.evaluate_sql(sql_statement)


def create_compliance_scorer(
    llm_endpoint: Optional[str] = None,
    name: str = "databricks_sql_compliance"
) -> DatabricksSQLComplianceScorer:
    """Create a Databricks SQL compliance scorer."""
    scorer = DatabricksSQLComplianceScorer(llm_endpoint=llm_endpoint)
    scorer.name = name
    return scorer
