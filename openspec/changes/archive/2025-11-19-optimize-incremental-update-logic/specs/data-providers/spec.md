## ADDED Requirements
### Requirement: Smart Download Capability
All data providers SHALL support intelligent download strategy that automatically determines whether to fetch full or incremental data.

#### Scenario: Smart download for new symbol (no database record)
- **GIVEN** symbol does NOT exist in database
- **WHEN** provider is called with symbol but NO start_date and end_date
- **THEN** the provider SHALL:
  1. Call provider API without start_date and end_date parameters
  2. Provider API SHALL fetch complete historical data for the symbol
  3. Return full historical dataset
  4. No need to query asset_basic for list_date

#### Scenario: Smart download for existing symbol
- **GIVEN** symbol exists in database (latest record: 2024-11-15)
- **WHEN** provider is called with symbol and no date parameters
- **THEN** the provider SHALL:
  1. Use the database query result provided by caller
  2. Calculate start_date as latest record + 1 day (2024-11-18)
  3. Calculate end_date as current date
  4. Fetch incremental data from 2024-11-18 to current date
  5. Return standardized DataFrame with incremental data

#### Scenario: Force update mode (ignore database)
- **GIVEN** force_update=True is set
- **WHEN** provider is called with symbol and force_update flag
- **THEN** the provider SHALL:
  1. Ignore database existing records
  2. Use caller-provided date range or API's full range
  3. Fetch data and allow overwriting existing records
  4. Return fetched data

#### Scenario: Support trade_date batch update (Tushare)
- **GIVEN** trade_date parameter is provided
- **WHEN** provider is called with trade_date but NO symbol
- **THEN** the provider SHALL:
  1. For Tushare: call API with trade_date parameter, no symbol
  2. Fetch all stocks data for the specified trade_date
  3. Return combined DataFrame for all stocks
  4. Skip database queries (not applicable for batch mode)

### Requirement: Latest Record Query Capability
Data providers SHALL provide methods to query database for latest records.

#### Scenario: Query latest daily record
- **GIVEN** provider has access to TimescaleDB database
- **WHEN** `get_latest_record()` is called with symbol and frequency='daily'
- **THEN** the provider SHALL:
  1. Execute SQL query: `SELECT * FROM symbol_daily WHERE symbol = ? ORDER BY time DESC LIMIT 1`
  2. Return the latest record as a DataFrame or None if no data exists
  3. Include all columns: time, symbol, open, high, low, close, volume, amount, adj_factor

#### Scenario: Query latest minute record
- **GIVEN** provider has access to TimescaleDB database
- **WHEN** `get_latest_record()` is called with symbol and frequency='minute_1'
- **THEN** the provider SHALL:
  1. Execute SQL query: `SELECT * FROM symbol_minute WHERE symbol = ? ORDER BY time DESC LIMIT 1`
  2. Return the latest minute record or None if no data exists

### Requirement: Intelligent Overwrite Decision
Data providers SHALL determine whether to overwrite existing records.

#### Scenario: Detect incomplete daily data
- **GIVEN** latest record timestamp is from today
- **WHEN** `should_overwrite_latest_record()` is called during trading hours
- **THEN** the provider SHALL:
  1. Check current time against trading hours (9:30-15:00)
  2. If current time is within trading hours AND latest record time is today, return True
  3. If current time is after trading hours, return False (today's data is complete)
  4. Consider market holidays and weekends in the calculation

#### Scenario: Detect complete daily data
- **GIVEN** latest record timestamp is from previous trading day
- **WHEN** `should_overwrite_latest_record()` is called
- **THEN** the provider SHALL:
  1. Return False (previous trading day data should not be overwritten)
  2. Allow normal incremental update from next trading day

### Requirement: Date Range Calculation
All providers SHALL implement intelligent date range calculation for different data types.

#### Scenario: Calculate date range for daily data
- **GIVEN** latest record timestamp and current timestamp
- **WHEN** `calculate_date_range()` is called for frequency='daily'
- **THEN** the method SHALL:
  1. If no latest record exists:
     - Return (None, None) to signal full historical data fetch
     - Provider API will fetch all available data when date parameters are empty
  2. If latest record exists:
     - Calculate next trading day after latest record
     - Return (next_trading_day, current_date)

#### Scenario: Calculate date range for minute data
- **GIVEN** latest minute record timestamp
- **WHEN** `calculate_date_range()` is called for frequency='minute_1'
- **THEN** the method SHALL:
  1. Calculate start_time as latest timestamp + 1 minute
  2. Calculate end_time as current time
  3. Return (start_time, end_time) for minute-level fetching

#### Scenario: Handle market holidays
- **GIVEN** latest record is before a market holiday
- **WHEN** calculating date range for daily data
- **THEN** the calculation SHALL:
  1. Skip market holidays in date range
  2. Use next trading day after holiday as start_date
  3. Ensure continuous data without gaps

### Requirement: Asset Basic Information Integration (Optional)
Data providers MAY integrate with asset_basic table for enhanced functionality, but this is optional. All other requirements SHALL follow standard behavior..

#### Scenario: Query active symbols for bulk update
- **GIVEN** provider supports asset_basic integration
- **WHEN** bulk update is requested for all active symbols
- **THEN** the provider MAY:
  1. Query asset_basic for symbols with list_status='L' (listed)
  2. Filter symbols based on market, industry, or other criteria
  3. Use the filtered symbol list for bulk updates
  4. This is optional - providers can use their own symbol filtering logic

#### Scenario: Skip asset_basic queries when not needed
- **GIVEN** provider API supports full data fetch without date constraints
- **WHEN** fetching data for new or existing symbols
- **THEN** the provider SHALL:
  1. NOT query asset_basic for list_date (not required)
  2. Call provider API with minimal parameters (symbol only, no dates)
  3. Let provider API determine the appropriate date range
  4. This simplifies the implementation and reduces database queries

## MODIFIED Requirements
### Requirement: Standardized Data Format (Enhanced)
All providers SHALL maintain consistent data formats AND support incremental update metadata.

#### Scenario: Include update metadata in incremental data
- **WHEN** provider returns incremental data
- **THEN** the data SHALL have:
  - Standard columns: time, symbol, open, high, low, close, volume, amount, adj_factor
  - Additional metadata (optional): update_type (full/incremental), source_timestamp
  - Same data types as specified in original requirement

#### Scenario: Handle empty incremental data
- **GIVEN** latest record timestamp is very recent
- **WHEN** incremental fetch returns no new data
- **THEN** the provider SHALL:
  1. Return empty DataFrame with correct schema
  2. Log that no new data is available
  3. Not raise an error
