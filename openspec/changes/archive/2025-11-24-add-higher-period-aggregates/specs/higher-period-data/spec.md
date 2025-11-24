# higher-period-data Specification

## Purpose
Provide pre-computed higher period (weekly, monthly) market data aggregates for efficient analysis and backtesting queries.

## ADDED Requirements

### Requirement: Weekly Price Data Aggregate
The system SHALL provide a continuous aggregate for weekly OHLCV data.

#### Scenario: Create symbol_weekly continuous aggregate
- **WHEN** database initialization completes
- **THEN** system SHALL create a TimescaleDB continuous aggregate `symbol_weekly` with:
  - time_bucket('1 week', time) as time dimension
  - symbol as grouping dimension
  - first(open, time) AS open
  - max(high) AS high
  - min(low) AS low
  - last(close, time) AS close
  - sum(volume) AS volume
  - sum(amount) AS amount
  - last(adj_factor, time) AS adj_factor
  - Materialization policy with 1 hour refresh interval
  - Index on (symbol, time DESC)

#### Scenario: Query weekly data through SDK
- **GIVEN** symbol_weekly aggregate exists and contains data
- **WHEN** user calls `fdh.get_weekly(['600519.SH'], '2024-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Query symbol_weekly aggregate
  - Return pandas DataFrame with columns: time, symbol, open, high, low, close, volume, amount, adj_factor
  - Return data in ascending time order
  - Complete query in < 100ms for single symbol, 1-year range

#### Scenario: Automatic refresh on new daily data
- **GIVEN** symbol_weekly aggregate exists
- **WHEN** new daily data is inserted into symbol_daily
- **THEN** system SHALL automatically refresh affected weekly buckets within 1 hour

### Requirement: Monthly Price Data Aggregate
The system SHALL provide a continuous aggregate for monthly OHLCV data.

#### Scenario: Create symbol_monthly continuous aggregate
- **WHEN** database initialization completes
- **THEN** system SHALL create a TimescaleDB continuous aggregate `symbol_monthly` with:
  - time_bucket('1 month', time) as time dimension
  - symbol as grouping dimension
  - first(open, time) AS open
  - max(high) AS high
  - min(low) AS low
  - last(close, time) AS close
  - sum(volume) AS volume
  - sum(amount) AS amount
  - last(adj_factor, time) AS adj_factor
  - Materialization policy with 1 hour refresh interval
  - Index on (symbol, time DESC)

#### Scenario: Query monthly data through SDK
- **GIVEN** symbol_monthly aggregate exists and contains data
- **WHEN** user calls `fdh.get_monthly(['000858.SZ'], '2020-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Query symbol_monthly aggregate
  - Return pandas DataFrame with OHLCV columns
  - Return data in ascending time order
  - Complete query in < 100ms for single symbol, 5-year range

### Requirement: Weekly Daily Basic Metrics Aggregate
The system SHALL provide a continuous aggregate for weekly aggregated daily basic metrics.

#### Scenario: Create daily_basic_weekly continuous aggregate
- **WHEN** database initialization completes
- **THEN** system SHALL create a TimescaleDB continuous aggregate `daily_basic_weekly` with:
  - time_bucket('1 week', time) as time dimension
  - symbol as grouping dimension
  - avg(turnover_rate) AS avg_turnover_rate
  - avg(volume_ratio) AS avg_volume_ratio
  - avg(pe) AS avg_pe
  - avg(pe_ttm) AS avg_pe_ttm
  - avg(pb) AS avg_pb
  - avg(ps) AS avg_ps
  - avg(ps_ttm) AS avg_ps_ttm
  - avg(dv_ratio) AS avg_dv_ratio
  - avg(dv_ttm) AS avg_dv_ttm
  - last(total_share, time) AS total_share
  - last(float_share, time) AS float_share
  - last(free_share, time) AS free_share
  - last(total_mv, time) AS total_mv
  - last(circ_mv, time) AS circ_mv
  - Materialization policy with 1 hour refresh interval
  - Index on (symbol, time DESC)

#### Scenario: Query weekly daily basic metrics
- **GIVEN** daily_basic_weekly aggregate exists and contains data
- **WHEN** user calls `fdh.get_daily_basic_weekly(['600519.SH'], '2024-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Query daily_basic_weekly aggregate
  - Return pandas DataFrame with aggregated metrics
  - Return data in ascending time order

### Requirement: Monthly Daily Basic Metrics Aggregate
The system SHALL provide a continuous aggregate for monthly aggregated daily basic metrics.

#### Scenario: Create daily_basic_monthly continuous aggregate
- **WHEN** database initialization completes
- **THEN** system SHALL create a TimescaleDB continuous aggregate `daily_basic_monthly` with:
  - time_bucket('1 month', time) as time dimension
  - symbol as grouping dimension
  - Averaged ratio metrics (turnover_rate, pe, pb, etc.)
  - Last values for share counts and market values
  - Materialization policy with 1 hour refresh interval
  - Index on (symbol, time DESC)

#### Scenario: Query monthly daily basic metrics
- **GIVEN** daily_basic_monthly aggregate exists and contains data
- **WHEN** user calls `fdh.get_daily_basic_monthly(['000858.SZ'], '2020-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Query daily_basic_monthly aggregate
  - Return pandas DataFrame with aggregated metrics
  - Return data in ascending time order

### Requirement: Aggregate Maintenance and Monitoring
The system SHALL provide tools for managing continuous aggregates.

#### Scenario: Manual refresh trigger
- **GIVEN** continuous aggregates exist
- **WHEN** user runs `fdh-cli refresh-aggregates --table symbol_weekly --start 2024-01-01 --end 2024-12-31`
- **THEN** system SHALL:
  - Execute CALL refresh_continuous_aggregate() for specified range
  - Log refresh progress
  - Return success/failure status

#### Scenario: Check aggregate status
- **GIVEN** continuous aggregates exist
- **WHEN** user runs `fdh-cli status --aggregates`
- **THEN** system SHALL display:
  - List of all continuous aggregates
  - Last refresh time for each
  - Refresh policy configuration
  - Aggregate storage size

#### Scenario: Data accuracy validation
- **GIVEN** symbol_weekly aggregate contains data
- **WHEN** comparing against manual Pandas resample of symbol_daily
- **THEN** aggregated OHLCV values SHALL match within 0.01% tolerance
