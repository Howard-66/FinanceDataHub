# cli-etl Specification

## Purpose
TBD - created by archiving change implement-phase2-core-batch-processing. Update Purpose after archive.
## Requirements
### Requirement: fdh-cli etl Command Enhancement
The `fdh-cli etl` command SHALL be enhanced to sync data from PostgreSQL to analytical storage (Parquet + DuckDB).

#### Scenario: Execute full ETL
- **WHEN** user runs `fdh-cli etl --from-date 2024-01-01`
- **THEN** the command SHALL:
  1. Connect to PostgreSQL database
  2. Extract data from TimescaleDB tables
  3. Transform data to optimized format
  4. Load to Parquet files partitioned by date/symbol
  5. Update DuckDB metadata
  6. Display summary statistics

#### Scenario: ETL with date range
- **WHEN** user runs `fdh-cli etl --from-date 2024-01-01 --to-date 2024-12-31`
- **THEN** the command SHALL:
  1. Extract data only from specified date range
  2. Create/update Parquet files for that period
  3. Skip data outside the range

#### Scenario: ETL for specific symbols
- **WHEN** user runs `fdh-cli etl --symbols 600519.SH,000858.SZ`
- **THEN** the command SHALL:
  1. Extract data only for specified symbols
  2. Create symbol-specific Parquet files
  3. Preserve partitioning by date

### Requirement: Parquet File Organization
The ETL process SHALL organize Parquet files for optimal query performance.

#### Scenario: Partition by date
- **WHEN** ETL processes daily data
- **THEN** Parquet files SHALL be organized in directory structure:
  ```
  data/parquet/daily/
    year=2024/
      month=01/
        day=01/
          symbol=600519.SH.parquet
          symbol=000858.SZ.parquet
  ```

#### Scenario: Partition by symbol
- **WHEN** ETL processes data
- **THEN** Parquet files SHALL also support symbol-based partitioning:
  ```
  data/parquet/symbol/
    600519.SH/
      2024/
        01/
          data.parquet
  ```

#### Scenario: Compression and optimization
- **WHEN** writing Parquet files
- **THEN** the system SHALL:
  - Use Zstd compression (compression level 3)
  - Use dictionary encoding for string columns
  - Use run-length encoding for repeated values
  - Optimize for Spark/DuckDB compatibility

### Requirement: DuckDB Integration
The system SHALL use DuckDB as the analytical query engine for Parquet files.

#### Scenario: Create DuckDB database
- **WHEN** ETL runs for first time
- **THEN** the system SHALL:
  1. Create DuckDB database file at configured path
  2. Register Parquet file locations as external tables
  3. Create indexes for common query patterns

#### Scenario: Query Parquet through DuckDB
- **WHEN** SDK queries analytical data
- **THEN** the query SHALL be executed against DuckDB
- **AND** DuckDB SHALL read directly from Parquet files
- **AND** no data duplication SHALL occur

#### Scenario: Incremental ETL
- **WHEN** ETL runs on existing Parquet files
- **THEN** the system SHALL:
  1. Detect existing partitions
  2. Extract only new/changed data from PostgreSQL
  3. Append to existing Parquet files or create new partitions
  4. Update DuckDB metadata

### Requirement: ETL Performance Optimization
The ETL process SHALL be optimized for large-scale data operations.

#### Scenario: Parallel processing
- **WHEN** ETL processes multiple symbols
- **THEN** the system SHALL use parallel processing
  - Default: CPU core count workers
  - Configurable via ETL settings

#### Scenario: Batch processing
- **WHEN** extracting large datasets
- **THEN** the system SHALL:
  - Use batch size from ETL configuration
  - Stream data to avoid memory overflow
  - Progress tracking for long operations

#### Scenario: Schema evolution
- **WHEN** database schema changes
- **THEN** ETL SHALL:
  - Detect schema changes automatically
  - Apply compatible changes to Parquet files
  - Log schema migration actions

### Requirement: Data Validation
The ETL process SHALL validate data integrity during transformation.

#### Scenario: Validate record counts
- **WHEN** ETL completes
- **THEN** the system SHALL:
  - Compare record counts between source and target
  - Report any discrepancies
  - Exit with error if mismatch exceeds threshold

#### Scenario: Validate data quality
- **WHEN** transforming data
- **THEN** the system SHALL check:
  - No null values in required columns
  - Price values are positive
  - Volume values are non-negative
  - Time stamps are monotonically increasing per symbol

#### Scenario: Generate validation report
- **WHEN** ETL completes successfully
- **THEN** a validation report SHALL be generated with:
  - Total records processed
  - Records per symbol
  - Date range covered
  - Data quality metrics
  - Any warnings or anomalies

