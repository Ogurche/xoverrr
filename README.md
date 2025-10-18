# xover (pronounced "crossover")
Designed for comparing data between sources with detailed analysis and discrepancy reporting.

## Key Features
- **Multiple DBMS Support**: Oracle, PostgreSQL, ClickHouse (extensible list via adapter layer) -- tables/views
- **Connection Versatility**: Requires SQLAlchemy Engine for source and target databases
- **Comparison Strategies**: Data sample comparison, count-only comparison with daily aggregates, and fully custom (raw) SQL queries
- **Smart Analysis**:
  * Excludes "fresh" data to account for replication lag, for example
  * Auto-detection of primary keys and column types from DBMS metadata catalog (PK must be found on at least one side, or specify your own via parameter)
  * Application-side type conversion
  * Auto-exclusion of columns with mismatched names from comparison
- **Optimization**: Two samples of 1 million rows with 10 columns each (each 330 MB), compared in 3 seconds (Intel Core i5/16GB)
- **Detailed Reporting**: Comprehensive analysis of column discrepancies with examples (column view/record view)
- **Flexible Configuration**: Field exclusion, tolerance thresholds, ability to specify "custom" primary key
- **Unit Tests**: Comparison methods, functional and performance tests

## Example Report
```
================================================================================
2025-10-15 20:05:55
DATA SAMPLE COMPARISON REPORT:
public.order_trans
VS
stage.acquiring_internet_order_trans
================================================================================
timezone: Europe/Athens

        SELECT id, customer_code, retailer_id, order_id, amount, created_at, updated_at, case when updated_at > (now() - INTERVAL '%(exclude_recent_hours)s hours') then 'y' end as xrecently_changed
        FROM public.order_trans
        WHERE 1=1
            AND created_at >= date_trunc('day', %(start_date)s::date)
            AND created_at < date_trunc('day', %(end_date)s::date)  + interval '1 days'

    params: {'exclude_recent_hours': 6, 'start_date': '2025-10-08', 'end_date': '2025-10-15'}
----------------------------------------

        SELECT id, customer_code, retailer_id, order_id, amount, created_at, updated_at, case when updated_at > (sysdate - :exclude_recent_hours) then 'y' end as xrecently_changed
        FROM stage.acquiring_internet_order_trans
        WHERE 1=1
            AND created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
            AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1

    params: {'exclude_recent_hours': 0.25, 'start_date': '2025-10-08', 'end_date': '2025-10-15'}
----------------------------------------

SUMMARY:
  Source rows: 261199
  Target rows: 261200
  Only source rows: 0
  Only target rows: 1
  Common rows (by primary key): 261199
  Totally matched rows: 261198
----------------------------------------
  Source only rows %: 0.00000
  Target only rows %: 0.00000
  Mismatched rows %: 0.00038
  Final discrepancies score: 0.00023
  Final data quality score: 99.99977
  Source-only key examples: None
  Target-only key examples: {'20000018'}
  Common attribute columns: customer_code, retailer_id, order_id, amount, created_at, updated_at
  Skipped source columns: term_date, payload, refer
  Skipped target columns: rrn, mt_change_date, termname

COLUMN DIFFERENCES:
  Discrepancies per column (max %): 0.00038
  Count of mismatches per column:

column_name  mismatch_count
 updated_at               1
  Some examples:

primary_key column_name source_value        target_value
30706884    updated_at  2025-10-10 11:15:09 2025-10-09 11:29:09

DISCREPANT DATA (first pairs):
Sorted by primary key and dataset:


id       customer_code retailer_id order_id amount created_at          updated_at          xflg
30706884 4485890     19823       N/A      350    2025-10-08 12:44:15 2025-10-10 11:15:09 src
30706884 4485890     19823       N/A      350    2025-10-08 12:44:15 2025-10-09 11:29:09 trg


================================================================================
```

## Weighted Metrics Calculation
**Final score formula**: `100 - final_diff_score`
- **final_diff_score** = (source_only_rows% × 0.2) + (target_only_rows% × 0.2) + (rows_mismatched_by_any_column% × 0.6)
- Scores 0-100%, higher = better data quality

## Comparison Methods

### 1. Data Sample Comparison (`compare_sample`) - suitable when you need to compare by row sets and column values over a date range

Compares data by row sets and column values over a date range.

```python
status, report, stats, details = comparator.compare_sample(
    source_table=TableReference("table_name", "schema_name"),
    target_table=TableReference("table_name", "schema_name"),
    date_column="created_at",
    update_column="modified_date",
    date_range=("2024-01-01", "2024-01-31"),
    exclude_columns=["audit_timestamp", "internal_id"],
    custom_primary_key=["id", "user_id"],
    tolerance_percentage=1.0,
    exclude_recent_hours=24,
    max_examples=3
)
```

**Parameters:**
- `source_table`, `target_table` - references to compared tables (`TableReference`)
- `date_column` - column for date range filtering
- `update_column` - column for identifying "fresh" data (excluded from comparison on both sides)
- `date_range` - tuple `(start_date, end_date)` in "YYYY-MM-DD" format
- `exclude_columns` - list of columns to exclude from comparison
- `custom_primary_key` - custom primary key (if not specified - determined automatically)
- `tolerance_percentage` - acceptable discrepancy percentage (0.0-100.0)
- `exclude_recent_hours` - exclude data modified in the last N hours
- `max_examples` - maximum number of discrepancy examples to include in report

### 2. Count Comparison (`compare_counts`) - for efficient processing of large volumes (over large date range) and localization of missing rows or duplicates

Compares daily record count aggregates. Efficient for large data volumes.

```python
status, report, stats, details = comparator.compare_counts(
    source_table=TableReference("users", "schema1"),
    target_table=TableReference("users", "schema2"),
    date_column="created_at",
    date_range=("2024-01-01", "2024-01-31"),
    tolerance_percentage=2.0,
    max_examples=5
)
```

**Parameters:**
- `source_table`, `target_table` - references to compared tables
- `date_column` - column for daily grouping
- `date_range` - date range for analysis
- `tolerance_percentage` - acceptable discrepancy percentage
- `max_examples` - maximum number of daily discrepancy examples to include in report

### 3. Custom Query Comparison (`compare_custom_query`)

Compares data from arbitrary SQL queries. Suitable for complex scenarios.

```python
status, report, stats, details = comparator.compare_custom_query(
    source_query="SELECT id as user_id, name as user_name, created_at as created_date FROM scott.source_table WHERE status = 'active'",
    source_params={},
    target_query="SELECT user_id, user_name, created_date FROM scott.target_table WHERE status = :status",
    target_params={'status': 'active'},
    custom_primary_key=["id"],
    exclude_columns=["internal_code"],
    tolerance_percentage=0.5,
    max_examples=3
)
```

**Parameters:**
- `source_query`, `target_query` - SQL queries for sources (support parameterization)
- `source_params`, `target_params` - query parameters
- `custom_primary_key` - mandatory parameter, list of columns
- `exclude_columns` - columns to exclude from comparison
- `tolerance_percentage` - acceptable discrepancy percentage
- `max_examples` - maximum number of discrepancy examples to include in report
- For automatic exclusion of recently changed records, add to SELECT query in `compare_custom_query` method:
```sql
case when updated_at > (sysdate - 3/24) then 'y' end as xrecently_changed
```

**Automatic Primary Key Detection:**
- If `custom_primary_key` is not specified, system automatically determines PK from metadata
- If different PKs in source and target, source PK will be used with warning

**Large Data Handling:**
- DataFrame size check (hard limit 3GB per sample)
- Efficient comparison via XOR properties
- Configurable limits via constants

**Return Values:**
All methods return tuple:
- `status` - comparison status (`COMPARISON_SUCCESS`/`COMPARISON_FAILED`/`COMPARISON_SKIPPED`)
- `report` - text report with discrepancy details
- `stats` - `ComparisonStats` object with comparison statistics, dataclass instance
- `details` - `ComparisonDiffDetails` object with discrepancy details and examples, dataclass instance

### Status Types
- **COMPARISON_SUCCESS**: Comparison passed within tolerance
- **COMPARISON_FAILED**: Discrepancies exceed tolerance threshold or technical error
- **COMPARISON_SKIPPED**: No data to compare (both tables empty)

### Convenient Logging
Structured logging with timings:
```
2024-01-15 10:30:45 - INFO - xover.core._compare_samples - Query executed in 2.34s
2024-01-15 10:30:46 - INFO - xover.core._compare_samples - Source: 150000 rows, Target: 149950 rows
2024-01-15 10:30:47 - INFO - xover.utils.compare_dataframes - Comparison completed in 1.2s
```

### Tolerance Percentage
- **tolerance_percentage**: Acceptable discrepancy threshold (0.0-100.0)
- If final_diff_score > tolerance: status = COMPARISON_FAILED
- If final_diff_score ≤ tolerance: status = COMPARISON_SUCCESS
- Allows configuring acceptable discrepancy level

### Usage Examples
**Sample Comparison:**
```python
from xover import DataQualityComparator, TableReference, COMPARISON_SUCCESS, COMPARISON_FAILED, COMPARISON_SKIPPED

def create_src_engine():
    connection_string = 'tbd'
    return engine

def create_trg_engine():
    connection_string = 'tbd'
    engine = create_engine(connection_string)
    return engine

comparator = DataQualityComparator(
    source_engine=src_engine,
    target_engine=trg_engine,
    timezone='Europe/Athens'
)

source = TableReference("users", "schema1")
target = TableReference("users", "schema2")

status, report, stats, details = comparator.compare_sample(
    source,
    target,
    date_column="created_at",
    update_column="modified_date",
    exclude_columns=["audit_timestamp", "internal_id"],
    exclude_recent_hours=24,
    tolerance_percentage=0
)
print(report)
if status == COMPARISON_FAILED:
    raise Exception(f"Sample check failed")
```