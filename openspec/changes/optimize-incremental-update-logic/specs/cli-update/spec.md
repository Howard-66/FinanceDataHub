## MODIFIED Requirements
### Requirement: Incremental Update Mode
The `fdh-cli update` command SHALL implement intelligent incremental update strategy.

#### Scenario: Update existing symbol with smart date range
- **GIVEN** database contains historical data for symbol 600519.SH
- **WHEN** user runs `fdh-cli update --symbols 600519.SH --mode incremental`
- **THEN** the command SHALL:
  1. Query database to find the latest record timestamp for the symbol
  2. Check if the latest record is intraday data (same day but incomplete)
  3. If latest record is from today after trading hours, fetch data from previous trading day to current time
  4. If latest record is from today during trading hours, decide whether to overwrite based on market status
  5. Calculate appropriate start_date and end_date for incremental fetch
  6. Fetch incremental data from providers using smart routing
  7. Insert/update data in TimescaleDB with proper upsert logic

#### Scenario: Initialize new symbol with full historical data
- **GIVEN** database does NOT contain data for symbol 600519.SH
- **WHEN** user runs `fdh-cli update --symbols 600519.SH --mode incremental`
- **THEN** the command SHALL:
  1. Detect that no existing data exists for the symbol
  2. Call provider API without start_date and end_date parameters
  3. Provider SHALL fetch complete historical data for the symbol
  4. Insert full historical dataset into database
  5. No need to query asset_basic for list_date

#### Scenario: Update all assets without specifying symbols
- **GIVEN** user wants to update all stocks for today
- **WHEN** user runs `fdh-cli update --dataset daily --mode incremental` (no --symbols)
- **THEN** the command SHALL:
  1. Use provider's trade_date capability (for Tushare)
  2. Call Tushare API with trade_date=current_date, no symbol parameter
  3. Fetch all stocks data for today in one API call
  4. Insert data into database for all fetched symbols
  5. For providers without trade_date support, use alternative bulk update strategy

#### Scenario: Use --dataset instead of --frequency
- **GIVEN** user wants to update daily basic data
- **WHEN** user runs `fdh-cli update --dataset daily_basic --mode incremental`
- **THEN** the command SHALL:
  1. Recognize that daily_basic is not a time frequency but a data type
  2. Fetch daily basic data for the specified symbols
  3. Apply appropriate incremental update logic based on data type
  4. The --dataset parameter replaces the less accurate --frequency parameter
  5. Legacy --frequency parameter remains supported for backward compatibility

#### Scenario: Handle multiple data types
- **GIVEN** user wants to update both daily price and daily basic data
- **WHEN** user runs `fdh-cli update --symbols 600519.SH --dataset daily,daily_basic --mode incremental`
- **THEN** the command SHALL:
  1. For daily (price data): apply time-based incremental logic
  2. For daily_basic (indicator data): apply appropriate incremental logic for non-time-series data
  3. Process each dataset type independently
  4. Report results for each dataset type separately
  5. Both --dataset and legacy --frequency parameters are accepted

#### Scenario: Fallback to full update on failure
- **WHEN** incremental update fails due to data provider errors
- **THEN** the command SHALL:
  1. Log the error details
  2. Attempt full refresh for the failed symbols
  3. Report both the incremental failure and full refresh success
  4. Exit with appropriate status code

#### Scenario: Daily data intraday handling
- **GIVEN** current time is within trading hours (9:30-15:00 on weekdays)
- **WHEN** incremental update fetches daily data for today
- **THEN** the system SHALL:
  1. Check if today's data already exists in database
  2. If yes, verify if it needs updating (e.g., partial day data)
  3. Fetch today's data and overwrite existing incomplete records
  4. Ensure OHLCV data represents the current state of the day

#### Scenario: Daily data after-hours handling
- **GIVEN** current time is after trading hours or on weekends
- **WHEN** incremental update processes daily data
- **THEN** the system SHALL:
  1. Fetch from the last complete trading day to current date
  2. Avoid fetching partial day data for current date
  3. Ensure all fetched data represents complete daily bars

### Requirement: Progress Display for Incremental Updates
The system SHALL provide detailed progress information showing incremental update decisions.

#### Scenario: Show incremental update rationale
- **WHEN** incremental update is in progress
- **THEN** the command SHALL display:
  - Symbol being processed
  - Last record timestamp found in database
  - Decision rationale (full vs incremental, date range calculated)
  - Number of new records fetched
  - Number of records overwritten (if applicable)

#### Scenario: Smart update mode with verbose logging
- **WHEN** user runs `fdh-cli update --smart-incremental --verbose`
- **THEN** the command SHALL display:
  - Database queries executed
  - Date range calculations performed
  - Provider selection decisions
  - Data validation results
