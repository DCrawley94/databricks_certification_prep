# Learning Materials Improvements

## Serverless Compute Compatibility

**Issue**: Some exercises require features unavailable on serverless compute (e.g., writing to `/tmp/`, `dbfs:/`, non-Delta formats for managed tables).

**Solutions**:
- Update exercises to use Unity Catalog managed tables with `.saveAsTable()` where appropriate
- For features genuinely unavailable on serverless, replace with theory-based questions testing understanding
- Add serverless limitation notes to exercise instructions where relevant
- Document workarounds (e.g., Delta format is default and required for UC managed tables)

**Examples**:
- Topic 2, Ex 13-14: Changed from `/tmp/` writes to `workspace.default.table_name` with `.saveAsTable()`
- Added note that `.partitionBy()` must precede `.saveAsTable()` in method chain

## Exam Scope Alignment

**Issue**: Some MCQs test concepts outside Associate exam scope (e.g., join strategies: broadcast, sort-merge, shuffle hash).

**Solutions**:
- Cross-reference all MCQs against official exam guide topics
- Mark Professional-level questions as "Advanced" or remove them
- Focus Associate MCQs on: join types (inner, left, outer, anti), not optimization strategies
- Verify UDF questions are scoped appropriately (UDFs are Professional-level)

**Examples**:
- MCQ 5 (join strategies) appears out of scope for Associate
- Associate should cover join syntax and types, not performance optimization internals

## Exercise Instructions Clarity

**Issue**: Some instructions assume specific implementation details that may not be necessary.

**Solutions**:
- Clarify when `.select()` is actually needed vs optional in DataFrame operations
- Explain implicit column selection in `.groupBy().agg()` patterns
- Document operation order requirements (e.g., `.partitionBy()` before `.saveAsTable()`)
- Add "why" context to exercises, not just "what" to implement

## Import Requirements

**Issue**: Exercise setup cells may be missing required imports for later exercises.

**Solutions**:
- Audit all setup cells to ensure imports match exercise requirements
- Add import statements before first use, not retroactively
- Example: `udf` import required for Exercise 12 but was missing from setup cell

## Documentation Gaps

**Issue**: Some Spark concepts (e.g., join strategies, optimization hierarchy) are tested but not covered in overview materials.

**Solutions**:
- Add sections to overview notebooks covering concepts that appear in MCQs
- If concept is out of scope, remove from practice materials
- Ensure overview coverage matches practice exercise requirements
- Example: Join strategies (broadcast, sort-merge, shuffle hash) not in Topic 1-2 overviews
