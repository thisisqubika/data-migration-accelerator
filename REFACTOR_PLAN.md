# Refactor Plan: src/artifact_translation_package

## Executive Summary

This document outlines a comprehensive refactoring plan for the `src/artifact_translation_package` codebase to enhance code quality, remove dead code, and modularize long functions.

## Analysis Summary

### Current State
- **Total Files Analyzed**: 25+ Python files
- **Lines of Code**: ~3,000+
- **Main Issues Identified**:
  - Code duplication across translation nodes (15+ translation files with similar patterns)
  - Long functions (>100 lines) in several files
  - Dead code and unused imports
  - Inconsistent error handling patterns
  - Redundant SQL cleaning logic across multiple files

### Refactoring Goals
1. **Eliminate Code Duplication**: Consolidate common patterns into reusable utilities
2. **Modularize Long Functions**: Break down functions >50 lines into smaller, focused units
3. **Remove Dead Code**: Delete unused imports, commented code, and redundant functions
4. **Improve Code Quality**: Apply consistent patterns, better error handling, and clearer abstractions
5. **Enhance Maintainability**: Make the codebase easier to understand and modify

---

## Detailed Refactoring Tasks

### Phase 1: Core Utilities & Common Patterns

#### 1.1 Consolidate Translation Node Logic
**Files Affected**: All files in `nodes/` directory (15+ files)

**Current Issues**:
- Each translation node (`tables_translation.py`, `views_translation.py`, etc.) has nearly identical structure
- Repetitive LLM invocation, error handling, and result building code
- Some nodes use `translation_helpers.py` while others don't

**Refactoring Actions**:
1. Enhance `utils/translation_helpers.py` with a generic translation function
2. Update all translation nodes to use the consolidated helper
3. Ensure consistent error handling across all nodes

**Expected Impact**:
- Reduce code duplication by ~60% in translation nodes
- Consistent behavior across all artifact types
- Easier to add new translation nodes

**Files to Modify**:
- `utils/translation_helpers.py` (enhance)
- `nodes/tables_translation.py` (refactor)
- `nodes/views_translation.py` (refactor)
- `nodes/procedures_translation.py` (refactor)
- `nodes/roles_translation.py` (refactor)
- `nodes/schemas_translation.py` (refactor)
- `nodes/databases_translation.py` (refactor)
- `nodes/stages_translation.py` (refactor)
- `nodes/streams_translation.py` (refactor)
- `nodes/pipes_translation.py` (refactor)
- `nodes/tags_translation.py` (refactor)
- `nodes/comments_translation.py` (refactor)
- `nodes/masking_policies_translation.py` (refactor)
- `nodes/udfs_translation.py` (refactor)
- `nodes/external_locations_translation.py` (refactor)
- `nodes/grants_translation.py` (refactor)

---

#### 1.2 Consolidate SQL Cleaning Logic
**Files Affected**: 
- `nodes/syntax_evaluation.py` (lines 56-144)
- `utils/llm_evaluation_utils.py` (lines 79-151)
- `databricks_job.py` (lines 318-332, 384-398)
- `main.py` (lines 134-165)

**Current Issues**:
- SQL cleaning logic duplicated in 4+ locations
- Markdown code block removal repeated
- Newline normalization repeated
- Inconsistent cleaning approaches

**Refactoring Actions**:
1. Create `utils/sql_cleaner.py` with centralized SQL cleaning functions:
   - `clean_sql_statement(sql: str) -> str`
   - `clean_sql_preview(sql: str, max_length: int = 200) -> str`
   - `normalize_newlines(sql: str) -> str`
   - `remove_markdown_code_blocks(sql: str) -> str`
2. Update all files to import and use these utilities
3. Remove duplicate implementations

**Expected Impact**:
- Single source of truth for SQL cleaning
- Consistent behavior across the codebase
- Easier to test and maintain

---

#### 1.3 Consolidate SQL File Saving Logic
**Files Affected**:
- `databricks_job.py` (lines 277-406)
- `main.py` (lines 168-207)

**Current Issues**:
- `save_sql_files()` duplicated in two files with slight variations
- `save_sql_files_dbutils()` only in databricks_job.py
- SQL formatting logic repeated

**Refactoring Actions**:
1. Create `utils/sql_file_writer.py` with:
   - `save_sql_files(result: Dict, output_path: str, use_dbutils: bool = False)`
   - `format_sql_content(artifact_type: str, sql_statements: List[str]) -> str`
   - `create_timestamped_output_dir(base_path: str) -> str`
2. Update both `databricks_job.py` and `main.py` to use the new utility
3. Remove duplicate implementations

**Expected Impact**:
- Eliminate ~130 lines of duplicate code
- Consistent SQL file generation
- Easier to add new output formats

---

### Phase 2: Modularize Long Functions

#### 2.1 Refactor `databricks_job.py`
**File**: `databricks_job.py` (517 lines)

**Long Functions to Break Down**:

1. **`process_translation_job()`** (lines 104-186, 83 lines)
   - Extract: `_setup_output_directory()`
   - Extract: `_process_all_batches()`
   - Extract: `_save_results_by_format()`

2. **`save_sql_files()`** (lines 277-338, 62 lines)
   - Extract: `_create_timestamped_output_dir()`
   - Extract: `_write_sql_file()`
   - Extract: `_clean_and_format_sql()`

3. **`save_sql_files_dbutils()`** (lines 341-406, 66 lines)
   - Extract: `_prepare_dbfs_output_path()`
   - Extract: `_write_sql_to_dbfs()`

4. **`databricks_entrypoint()`** (lines 472-514, 43 lines)
   - Extract: `_get_volume_path()`
   - Extract: `_get_job_parameters()`
   - Extract: `_print_job_summary()`

**Expected Impact**:
- Each function <50 lines
- Clearer separation of concerns
- Easier to test individual components

---

#### 2.2 Refactor `graph_builder.py`
**File**: `graph_builder.py` (444 lines)

**Long Functions to Break Down**:

1. **`TranslationGraph.__init__()`** (lines 236-321, 86 lines)
   - Extract: `_initialize_observability()`
   - Extract: `_create_graph_nodes()`
   - Extract: `_configure_graph_edges()`
   - Extract: `_compile_graph()`

2. **`TranslationGraph.run_batches()`** (lines 360-425, 66 lines)
   - Extract: `_initialize_merged_result()`
   - Extract: `_merge_batch_results()`
   - Extract: `_aggregate_metadata()`

**Expected Impact**:
- More readable initialization logic
- Easier to understand graph structure
- Better testability

---

#### 2.3 Refactor `syntax_evaluation.py`
**File**: `nodes/syntax_evaluation.py` (651 lines)

**Long Functions to Break Down**:

1. **`validate_sql_syntax()`** (lines 169-247, 79 lines)
   - Extract: `_parse_sql_with_sqlglot()`
   - Extract: `_transpile_sql()`
   - Extract: `_handle_parse_error()`
   - Extract: `_handle_transpile_error()`

2. **`evaluate_sql_compliance_with_llm()`** (lines 346-401, 56 lines)
   - Extract: `_collect_evaluable_statements()`
   - Extract: `_process_llm_batches()`
   - Extract: `_build_validation_summary()`

3. **`evaluate_sql_compliance()`** (lines 404-448, 45 lines)
   - Extract: `_validate_single_statement()`
   - Extract: `_collect_validation_results()`

4. **`build_evaluation_batch_data()`** (lines 468-519, 52 lines)
   - Extract: `_filter_evaluated_items()`
   - Extract: `_build_batch_info()`
   - Extract: `_build_translation_info()`
   - Extract: `_build_validation_info()`

5. **`evaluate_batch()`** (lines 583-650, 68 lines)
   - Extract: `_check_validation_enabled()`
   - Extract: `_select_validation_method()`
   - Extract: `_persist_if_evaluated()`

**Expected Impact**:
- Each function <50 lines
- Clearer validation flow
- Easier to add new validation methods

---

#### 2.4 Refactor `main.py`
**File**: `main.py` (344 lines)

**Long Functions to Break Down**:

1. **`process_single_file()`** (lines 24-81, 58 lines)
   - Extract: `_create_batches_from_file()`
   - Extract: `_process_batches()`
   - Extract: `_aggregate_single_result()`

2. **`process_multiple_files()`** (lines 84-118, 35 lines)
   - Extract: `_create_all_batches()`
   - Extract: `_run_batch_processing()`

3. **`save_results()`** (lines 210-240, 31 lines)
   - Extract: `_create_results_directory()`
   - Extract: `_save_json_results()`
   - Extract: `_save_sql_results()`

4. **`main()`** (lines 243-339, 97 lines)
   - Extract: `_parse_arguments()`
   - Extract: `_setup_output_directory()`
   - Extract: `_process_input_files()`
   - Extract: `_display_results()`
   - Extract: `_save_output_by_format()`

**Expected Impact**:
- Clearer main execution flow
- Easier to test individual steps
- Better separation of CLI logic from processing logic

---

### Phase 3: Remove Dead Code & Clean Up

#### 3.1 Remove Unused Imports
**Files Affected**: Multiple files

**Actions**:
1. Scan all Python files for unused imports
2. Remove unused imports from:
   - `databricks_job.py`: Check `re` usage (line 21)
   - `graph_builder.py`: Check `Annotated` usage (line 1)
   - `syntax_evaluation.py`: Check all imports
   - `llm_evaluation_utils.py`: Check all imports
   - All translation nodes: Check for unused imports

**Expected Impact**:
- Cleaner import statements
- Faster import times
- Clearer dependencies

---

#### 3.2 Remove Commented Code
**Files Affected**: Multiple files

**Actions**:
1. Remove commented-out code blocks
2. Remove placeholder comments like `# TODO`, `# FIXME` without actual tasks
3. Remove dummy comments that don't add value

**Specific Locations**:
- `graph_builder.py` line 117: Missing return statement in `translate_roles_node()`
- `graph_builder.py` lines 270-271, 309: Comments about file_formats being excluded
- `router.py` lines 23-24, 36-37: Comments about file_formats exclusion
- `syntax_evaluation.py` line 36: Comment about sequences being no longer processed
- `file_processor.py` line 36: Comment about sequences

**Expected Impact**:
- Cleaner codebase
- Less confusion about what's active
- Better maintainability

---

#### 3.3 Remove Redundant Functions
**Files Affected**: Multiple files

**Actions**:

1. **Duplicate SQL cleaning functions**:
   - Remove `clean_sql_statement()` from `syntax_evaluation.py` (after consolidating)
   - Remove `clean_sql_preview()` from `syntax_evaluation.py` (after consolidating)
   - Remove `clean_sql_preview()` from `llm_evaluation_utils.py` (after consolidating)

2. **Duplicate validation helpers**:
   - Remove `should_skip_sql_statement()` from `syntax_evaluation.py` (keep in `llm_evaluation_utils.py`)
   - Consolidate validation configuration access

3. **Unused or redundant utility functions**:
   - Review `utils/llm_utils.py`: `validate_node_requirements()` and `test_llm_connection()` - check if used
   - Review `utils/translation_helpers.py`: Ensure all functions are used

**Expected Impact**:
- Smaller codebase
- Clearer API surface
- Less maintenance burden

---

#### 3.4 Clean Up Configuration
**File**: `config/ddl_config.py` (331 lines)

**Actions**:
1. Remove `sequences_translator` configuration (line 163-171) - sequences are no longer processed
2. Consolidate LLM configuration - all translators use identical config
3. Extract common LLM parameters to avoid repetition
4. Remove unused configuration sections if any

**Expected Impact**:
- Cleaner configuration
- Easier to maintain
- Less confusion about supported features

---

### Phase 4: Improve Code Quality

#### 4.1 Standardize Error Handling
**Files Affected**: All translation nodes

**Current Issues**:
- Inconsistent error handling patterns
- Some nodes use `translation_helpers`, others don't
- Different error message formats

**Refactoring Actions**:
1. Ensure all translation nodes use consistent error handling
2. Use `translation_helpers.process_artifact_translation()` where applicable
3. Standardize error message format
4. Add proper error context in all cases

**Expected Impact**:
- Consistent error reporting
- Easier debugging
- Better user experience

---

#### 4.2 Add Type Hints
**Files Affected**: Multiple files

**Current Issues**:
- Some functions lack type hints
- Inconsistent type hint usage
- Missing return type annotations

**Refactoring Actions**:
1. Add type hints to all public functions
2. Ensure consistent type hint style
3. Use `Optional` and `Union` appropriately
4. Add type hints to private helper functions

**Priority Files**:
- `utils/file_processor.py`
- `utils/llm_utils.py`
- `utils/output_utils.py`
- All translation nodes

**Expected Impact**:
- Better IDE support
- Catch type errors earlier
- Improved documentation

---

#### 4.3 Improve Documentation
**Files Affected**: All files

**Current Issues**:
- Some docstrings are missing or incomplete
- Inconsistent docstring format
- Missing parameter descriptions

**Refactoring Actions**:
1. Ensure all public functions have complete docstrings
2. Use Google-style or NumPy-style docstrings consistently
3. Add examples for complex functions
4. Document all parameters and return values

**Expected Impact**:
- Better code documentation
- Easier onboarding for new developers
- Improved API reference

---

#### 4.4 Add Logging Improvements
**Files Affected**: Multiple files

**Current Issues**:
- Inconsistent logging levels
- Missing context in log messages
- Some functions lack logging entirely

**Refactoring Actions**:
1. Add appropriate logging to all major operations
2. Include context in log messages (batch size, artifact type, etc.)
3. Use consistent log levels (DEBUG, INFO, WARNING, ERROR)
4. Add structured logging where beneficial

**Priority Files**:
- `databricks_job.py`
- `main.py`
- `graph_builder.py`
- All translation nodes

**Expected Impact**:
- Better observability
- Easier debugging
- Improved production monitoring

---

### Phase 5: Graph Builder Improvements

#### 5.1 Simplify Node Registration
**File**: `graph_builder.py`

**Current Issues**:
- Manual node registration (lines 254-273)
- Repetitive edge configuration (lines 304-315)
- Hardcoded node names in multiple places

**Refactoring Actions**:
1. Create a node registry system
2. Auto-generate translation nodes from configuration
3. Simplify edge configuration using loops
4. Extract node name constants

**Expected Impact**:
- Easier to add new translation nodes
- Less boilerplate code
- Clearer graph structure

---

#### 5.2 Fix Missing Return Statement
**File**: `graph_builder.py`, line 117

**Current Issue**:
```python
def translate_roles_node(state: TranslationState) -> TranslationState:
    """Translate role artifacts."""
    if not state["batch"]:
        return state
    result = translate_roles(state["batch"])
    # Missing return statement!
```

**Refactoring Actions**:
1. Add missing return statement
2. Verify all translation nodes have proper returns

**Expected Impact**:
- Fix potential bug
- Consistent behavior across nodes

---

## Implementation Priority

### High Priority (Phase 1)
1. Consolidate translation node logic
2. Consolidate SQL cleaning logic
3. Consolidate SQL file saving logic

**Rationale**: These changes eliminate the most code duplication and provide the foundation for other improvements.

### Medium Priority (Phase 2)
4. Modularize long functions in `databricks_job.py`
5. Modularize long functions in `syntax_evaluation.py`
6. Modularize long functions in `main.py`

**Rationale**: These improve code readability and maintainability without changing behavior.

### Medium Priority (Phase 3)
7. Remove unused imports
8. Remove commented code
9. Remove redundant functions
10. Clean up configuration

**Rationale**: These clean up the codebase and reduce confusion.

### Low Priority (Phase 4)
11. Standardize error handling
12. Add type hints
13. Improve documentation
14. Add logging improvements

**Rationale**: These improve code quality but are less critical than structural changes.

### Low Priority (Phase 5)
15. Simplify node registration
16. Fix missing return statement

**Rationale**: These are nice-to-have improvements that can be done incrementally.

---

## Testing Strategy

### Unit Tests
- Create unit tests for new utility functions
- Test SQL cleaning functions with various inputs
- Test translation helper functions
- Test modularized functions

### Integration Tests
- Ensure translation nodes still work correctly after refactoring
- Test end-to-end workflows
- Verify output format consistency

### Regression Tests
- Run existing test suite before and after changes
- Compare outputs to ensure no behavioral changes
- Test with real artifact data

---

## Risk Assessment

### Low Risk
- Removing unused imports
- Removing commented code
- Adding type hints
- Improving documentation

### Medium Risk
- Consolidating SQL cleaning logic (ensure all edge cases covered)
- Modularizing long functions (ensure no behavior changes)
- Standardizing error handling (ensure error messages remain useful)

### High Risk
- Consolidating translation node logic (thorough testing required)
- Refactoring graph builder (could affect all workflows)
- Changing configuration structure (could break existing deployments)

**Mitigation**:
- Comprehensive testing before merging
- Gradual rollout with feature flags if needed
- Maintain backward compatibility where possible
- Clear communication of breaking changes

---

## Success Metrics

### Code Quality Metrics
- Reduce code duplication by 40%+
- Reduce average function length to <50 lines
- Remove all unused imports and dead code
- Achieve 80%+ type hint coverage

### Maintainability Metrics
- Reduce cyclomatic complexity
- Improve code readability scores
- Reduce technical debt
- Improve test coverage

### Developer Experience
- Faster onboarding for new developers
- Easier to add new translation nodes
- Clearer code structure
- Better documentation

---

## Timeline Estimate

### Phase 1: Core Utilities (3-4 days)
- Consolidate translation node logic: 1-2 days
- Consolidate SQL cleaning logic: 0.5 day
- Consolidate SQL file saving logic: 0.5 day
- Testing: 1 day

### Phase 2: Modularize Functions (4-5 days)
- Refactor `databricks_job.py`: 1 day
- Refactor `graph_builder.py`: 1 day
- Refactor `syntax_evaluation.py`: 1.5 days
- Refactor `main.py`: 1 day
- Testing: 0.5 day

### Phase 3: Remove Dead Code (2-3 days)
- Remove unused imports: 0.5 day
- Remove commented code: 0.5 day
- Remove redundant functions: 1 day
- Clean up configuration: 0.5 day
- Testing: 0.5 day

### Phase 4: Improve Code Quality (3-4 days)
- Standardize error handling: 1 day
- Add type hints: 1 day
- Improve documentation: 1 day
- Add logging improvements: 0.5 day
- Testing: 0.5 day

### Phase 5: Graph Builder (1-2 days)
- Simplify node registration: 1 day
- Fix missing return statement: 0.5 day
- Testing: 0.5 day

**Total Estimated Time**: 13-18 days

---

## Next Steps

1. **Review and Approve**: Review this plan with the team and get approval
2. **Create Branch**: Create a feature branch for refactoring work
3. **Implement Phase 1**: Start with high-priority consolidation work
4. **Test Thoroughly**: Ensure all changes are well-tested
5. **Iterate**: Move through phases incrementally
6. **Document**: Update documentation as changes are made
7. **Review**: Conduct code reviews for each phase
8. **Merge**: Merge changes incrementally to reduce risk

---

## Appendix: File-by-File Summary

### Files Requiring Major Refactoring
1. `databricks_job.py` (517 lines) - Multiple long functions, duplicate SQL saving
2. `graph_builder.py` (444 lines) - Long initialization, manual node registration
3. `syntax_evaluation.py` (651 lines) - Multiple long functions, duplicate SQL cleaning
4. `main.py` (344 lines) - Long main function, duplicate SQL saving
5. `config/ddl_config.py` (331 lines) - Repetitive LLM config, unused sections

### Files Requiring Moderate Refactoring
6. `utils/llm_evaluation_utils.py` (319 lines) - Duplicate SQL cleaning
7. `utils/file_processor.py` (208 lines) - Generally good, minor improvements
8. `utils/translation_helpers.py` (76 lines) - Needs enhancement for consolidation
9. All translation nodes (15 files, ~50 lines each) - Need consolidation

### Files Requiring Minor Refactoring
10. `utils/llm_utils.py` (92 lines) - Check for unused functions
11. `utils/output_utils.py` (87 lines) - Generally good
12. `utils/error_handler.py` (141 lines) - Generally good
13. `utils/observability.py` (114 lines) - Generally good
14. `nodes/router.py` (61 lines) - Minor cleanup
15. `nodes/aggregator.py` (67 lines) - Minor cleanup

---

## Conclusion

This refactoring plan provides a comprehensive roadmap for improving the `src/artifact_translation_package` codebase. By following this plan systematically, we can achieve significant improvements in code quality, maintainability, and developer experience while minimizing risk through incremental changes and thorough testing.

The plan prioritizes high-impact changes that eliminate duplication and improve structure, followed by quality improvements and cleanup. Each phase builds on the previous one, ensuring a smooth refactoring process.