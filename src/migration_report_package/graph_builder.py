from typing import Any, Dict, List, Optional, Annotated, TypedDict
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, END, START
from langchain_core.runnables import RunnableConfig
from report_llm import generate_report
from datetime import datetime
import os

import json


class MigrationState(TypedDict):
    """State for the migration graph execution."""
    input_dir: str
    latest_dir: str
    raw: Dict[str, Any]
    cleaned_raw: Dict[str, Any]
    count: Dict[str, Any]
    json_report: Dict[str, Any]
    md_report: str
    
def input_node(state: MigrationState) -> MigrationState:
    """Input node for the migration graph."""
    input_path = state["input_dir"]
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Output folder not found: {input_path}")
    ## Get output with most recent timestamp
    output_dirs = []
    for name in os.listdir(input_path):
        res = os.path.join(input_path, name)
        if not os.path.isdir(res):
            continue
        run_dt = datetime.strptime(name, "%Y-%m-%dT%H-%M-%SZ")
        output_dirs.append((run_dt, res))
    
    _ , latest = max(output_dirs, key=lambda x: x[0])
    state["latest_dir"] = latest
    ## Get translation results and evaluation notes
    raw = {"translation_results": [], "evaluation": []}
    for name in os.listdir(latest):
        out = os.path.join(latest, name)
        if os.path.isdir(out):
            for file_path in os.listdir(out):
                file = os.path.join(out, file_path)
                if "evaluation" in os.path.basename(file).lower():
                    with open(file, "r", encoding="utf-8") as f:
                        raw["evaluation"].append(json.load(f))
        else:
            if "translation_results.json" in os.path.basename(out).lower():
                with open(out, "r", encoding="utf-8") as f:
                    raw["translation_results"].append(json.load(f))
    state["raw"] = raw
    return state

def clean_raw(obj: Any) -> Any:
    if obj is None:
        return None

    # Handle dicts
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            pruned = clean_raw(v)
            if pruned not in (None, {}, [], ""):
                cleaned[k] = pruned
        return cleaned or None

    # Handle lists
    if isinstance(obj, list):
        cleaned = []
        for item in obj:
            pruned = clean_raw(item)
            if pruned not in (None, {}, [], ""):
                cleaned.append(pruned)
        return cleaned or None

    # Handle strings (remove empty and prune to MAX_LEN length)
    MAX_LEN = 150
    if isinstance(obj, str):
        s = obj.strip()
        if not s:
            return None
        return s if len(s) <= MAX_LEN else s[:MAX_LEN] + "…"

    return obj


def clean_raw_node(state: MigrationState) -> MigrationState:  
    """clean raw data by removing empty values and pruning long strings"""
    state["cleaned_raw"] = clean_raw(state["raw"])
    return state

def count_node(state: MigrationState) -> MigrationState:
    """Count trnslated artifacts, errors, warnings and validation errors for the report."""
    count = {"artifact_type": {}, "migration_errors": 0, "migration_warnings": 0, "successes": 0, "validation_errors": 0}
    for trans in state.get("cleaned_raw", {}).get("translation_results", []):
        for type, value in trans.get("observability", {}).get("artifact_counts", {}).items():
            if count["artifact_type"].get(type) is None:
                count["artifact_type"][type] = value
                count["successes"] += value
            else:
                count["artifact_type"][type] += value
                count["successes"] += value
        count["migration_errors"] += trans["observability"]["total_errors"]
        count["migration_warnings"] += trans["observability"]["total_warnings"]
    for eval in state.get("cleaned_raw", {}).get("evaluation", []):
        for res in eval.get("validation", {}).get("results", []):
            count["validation_errors"] += (1 if not res.get("syntax_valid", True) else 0)
    state["count"] = count
    return state

def report_node(state: MigrationState) -> MigrationState:
    """Create report with LLM."""
    result = generate_report(state["cleaned_raw"], state["count"])
    return {**state, "md_report": result, "json_report": state["count"]}

class MigrationReportGraph:
    def __init__(self, run_id: Optional[str] = None):
        """
        Initialize migration report graph.
        
        Args:
            run_id: Unique identifier for this run
        """
        # Create the StateGraph
        self.graph = StateGraph(MigrationState)

        # Add nodes
        self.graph.add_node("input", input_node)
        self.graph.add_node("clean", clean_raw_node)
        self.graph.add_node("count", count_node)
        self.graph.add_node("report", report_node)

        self.graph.add_edge(START, "input")
        self.graph.add_edge("input", "clean")
        self.graph.add_edge("clean", "count")   
        self.graph.add_edge("count", "report")
        self.graph.add_edge("report", END)

        # Compile the graph
        self.compiled_graph = self.graph.compile()

    def run(self, input_path: str) -> Dict[str, Any]:        
        try:
            initial_state: MigrationState = {
            "input_dir": input_path,
            "latest_dir": None,
            "raw": [],
            "count": None,
            "json_report": None,
            "md_report": None
        }

            final_state = self.compiled_graph.invoke(initial_state)
            report = final_state["md_report"] or {}
            json_report = final_state["json_report"] or {}
            latest_dir = final_state["latest_dir"] or {}
            return report[0], json_report, latest_dir

        except Exception as e:
            raise

