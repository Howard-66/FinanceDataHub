# sdk-data-layer Specification

## Purpose
TBD - created by archiving change implement-phase3-sdk-datalayer. Update Purpose after archive.
## Requirements
### Requirement: Daily OHLCV Data Queries
The FinanceDataHub SDK SHALL provide methods to query daily OHLCV data from PostgreSQL backend.

#### Scenario: Query daily data from PostgreSQL
- **GIVEN** PostgreSQL contains daily data
- **WHEN** user calls `fdh.get_daily(['600519.SH', '000858.SZ'], '2024-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Query symbol_daily table directly from PostgreSQL
  - Return pandas DataFrame with columns: time, symbol, open, high, low, close, volume, amount, adj_factor
  - Filter by time range (inclusive)
  - Filter by symbols list
  - Return data in ascending time order
  - Complete query in < 200ms for 2 symbols, 1-year range

#### Scenario: Async daily data query
- **GIVEN** FinanceDataHub is initialized
- **WHEN** user calls `await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Execute query asynchronously using asyncpg
  - Return same result format as synchronous call
  - Support concurrent queries without blocking

### Requirement: Minute-level OHLCV Data Queries
The FinanceDataHub SDK SHALL provide methods to query minute-level OHLCV data from PostgreSQL backend.

#### Scenario: Query minute data from PostgreSQL
- **GIVEN** PostgreSQL contains minute data
- **WHEN** user calls `fdh.get_minute(['600519.SH'], '2024-11-01', '2024-11-30', 'minute_1')`
- **THEN** system SHALL:
  - Query symbol_minute table from PostgreSQL with frequency filter
  - Return pandas DataFrame with columns: time, symbol, open, high, low, close, volume, amount, frequency
  - Filter by symbols, time range, and frequency
  - Return data in ascending time order
  - Support frequency parameter for 1m, 5m, 15m, 30m, 60m intervals
  - Complete query in < 500ms for 1 symbol, 1-month range

#### Scenario: Query multiple minute frequencies
- **GIVEN** PostgreSQL contains minute data for multiple frequencies
- **WHEN** user calls `fdh.get_minute(['000858.SZ'], '2024-11-01', '2024-11-30', 'minute_5')`
- **THEN** system SHALL:
  - Query symbol_minute table with frequency='minute_5'
  - Return DataFrame with 5-minute aggregated data
  - Support all standard frequencies: 1m, 5m, 15m, 30m, 60m

#### Scenario: Async minute data query
- **GIVEN** FinanceDataHub is initialized with async mode
- **WHEN** user calls `await fdh.get_minute_async(['000858.SZ'], '2024-11-01', '2024-11-30', 'minute_5')`
- **THEN** system SHALL:
  - Execute query asynchronously
  - Return DataFrame with minute-level data
  - Support frequency parameter for 1min, 5min, 15min, 30min intervals

### Requirement: Daily Basic Metrics Queries
The FinanceDataHub SDK SHALL provide methods to query daily basic metrics data from PostgreSQL backend.

#### Scenario: Query daily basic data
- **GIVEN** PostgreSQL contains daily_basic data
- **WHEN** user calls `fdh.get_daily_basic(['600519.SH'], '2024-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Query daily_basic table from PostgreSQL
  - Return pandas DataFrame with columns: time, symbol, turnover_rate, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv
  - Filter by symbols and time range
  - Return data in ascending time order
  - Complete query in < 300ms for 2 symbols, 1-year range

#### Scenario: Async daily basic query
- **GIVEN** FinanceDataHub is initialized
- **WHEN** user calls `await fdh.get_daily_basic_async(['600519.SH'], '2024-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Execute query asynchronously using asyncpg
  - Return same result format as synchronous call
  - Support concurrent queries without blocking

### Requirement: Adj Factor Queries
The FinanceDataHub SDK SHALL provide methods to query adjustment factor data from PostgreSQL backend.

#### Scenario: Query adj factor data
- **GIVEN** PostgreSQL contains adj_factor data
- **WHEN** user calls `fdh.get_adj_factor(['600519.SH'], '2020-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Query adj_factor table from PostgreSQL
  - Return pandas DataFrame with columns: time, symbol, adj_factor
  - Filter by symbols and time range
  - Return data in ascending time order
  - Complete query in < 200ms for 2 symbols, 5-year range

#### Scenario: Async adj factor query
- **GIVEN** FinanceDataHub is initialized
- **WHEN** user calls `await fdh.get_adj_factor_async(['600519.SH'], '2020-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Execute query asynchronously using asyncpg
  - Return same result format as synchronous call
  - Support concurrent queries without blocking

### Requirement: Basic Stock Information Queries
The FinanceDataHub SDK SHALL provide methods to query basic stock information (non-time-series) from PostgreSQL backend.

#### Scenario: Query basic stock information
- **GIVEN** PostgreSQL contains asset_basic data
- **WHEN** user calls `fdh.get_basic(['600519.SH', '000858.SZ'])`
- **THEN** system SHALL:
  - Query asset_basic table from PostgreSQL
  - Return pandas DataFrame with columns: ts_code, symbol, name, area, industry, market, exchange, list_status, list_date, delist_date, is_hs
  - Filter by symbols list (if provided)
  - Return data in ascending symbol order
  - Complete query in < 100ms for 10 symbols

#### Scenario: Query all basic information
- **GIVEN** PostgreSQL contains asset_basic data
- **WHEN** user calls `fdh.get_basic()` without symbols parameter
- **THEN** system SHALL:
  - Query all asset_basic records from PostgreSQL
  - Return pandas DataFrame with all columns
  - Return data in ascending ts_code order
  - Complete query in < 2s for all stocks

#### Scenario: Async basic information query
- **GIVEN** FinanceDataHub is initialized
- **WHEN** user calls `await fdh.get_basic_async(['600519.SH'])`
- **THEN** system SHALL:
  - Execute query asynchronously using asyncpg
  - Return same result format as synchronous call
  - Support concurrent queries without blocking

### Requirement: Simplified Backend Management
The FinanceDataHub SDK SHALL use PostgreSQL as the primary and only backend for data queries.

#### Scenario: Query without backend specification
- **GIVEN** FinanceDataHub is initialized
- **WHEN** user calls `fdh.get_daily(['600519.SH'], '2024-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Default to PostgreSQL backend
  - Query appropriate table directly
  - Return results with minimal latency

#### Scenario: Query with explicit backend
- **GIVEN** FinanceDataHub is initialized
- **WHEN** user calls `fdh.get_daily(['600519.SH'], '2024-01-01', '2024-12-31', backend='postgresql')`
- **THEN** system SHALL:
  - Use PostgreSQL backend as specified
  - Query appropriate table directly
  - Return same results as default query

### Requirement: Smart Routing Integration
The FinanceDataHub SDK SHALL integrate SmartRouter for intelligent data source selection.

#### Scenario: Route daily data query
- **GIVEN** SmartRouter is configured in sources.yml with multiple providers
- **WHEN** user calls `fdh.get_daily(['600519.SH'], '2024-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Use SmartRouter to select appropriate provider (if update needed)
  - Use SmartRouter to check data source availability
  - Log routing decisions for debugging
  - Support failover if primary data source fails

#### Scenario: SmartRouter availability check
- **GIVEN** SmartRouter is integrated with SDK
- **WHEN** user queries data that may need updating
- **THEN** system SHALL:
  - Check with SmartRouter if data source is available
  - Recommend update if data is stale
  - Proceed with query from PostgreSQL if data is current
  - Log all routing decisions

