## MODIFIED Requirements
### Requirement: Smart Download Mode (Default)
The `fdh-cli update` command SHALL implement intelligent download strategy by default.

#### Scenario: Smart download for new symbol (no database record)
- **GIVEN** database does NOT contain data for symbol 600519.SH
- **WHEN** user runs `fdh-cli update --dataset daily --symbols 600519.SH`
- **THEN** the command SHALL:
  1. Query database and detect no existing data for the symbol
  2. Call provider API WITHOUT start_date and end_date parameters
  3. Provider SHALL fetch complete historical data for the symbol
  4. Insert full historical dataset into database

#### Scenario: Smart download for existing symbol
- **GIVEN** database contains historical data for symbol 600519.SH (latest: 2024-11-15)
- **WHEN** user runs `fdh-cli update --dataset daily --symbols 600519.SH`
- **THEN** the command SHALL:
  1. Query database to find the latest record timestamp (2024-11-15)
  2. Calculate next trading day (2024-11-18)
  3. Fetch data from 2024-11-18 to current date (incremental)
  4. Insert/update data in database with upsert logic

#### Scenario: Smart download for all assets (no symbols specified)
- **GIVEN** user wants to update all stocks
- **WHEN** user runs `fdh-cli update --dataset daily` (no --symbols)
- **THEN** the command SHALL:
  1. Query database to get all symbols
  2. For each symbol, apply smart download logic
  3. Process symbols sequentially or in batches to avoid rate limits

#### Scenario: Trade date batch update (Tushare specific)
- **GIVEN** user wants to update all stocks for a specific date
- **WHEN** user runs `fdh-cli update --dataset daily --trade-date 2024-11-18`
- **THEN** the command SHALL:
  1. Use Tushare's trade_date capability
  2. Call Tushare API with trade_date=20241118, no symbol parameter
  3. Fetch all stocks data for 2024-11-18 in one API call
  4. Insert data into database for all fetched symbols
  5. Skip non-Tushare providers or use alternative strategy

#### Scenario: Force update mode
- **GIVEN** user wants to force refresh all data for a symbol
- **WHEN** user runs `fdh-cli update --dataset daily --symbols 600519.SH --force`
- **THEN** the command SHALL:
  1. Ignore database existing records
  2. Use user-specified date range or default to full range
  3. Fetch and overwrite all data in the specified range
  4. Useful for data cleanup or correction

#### Scenario: Force update with date range
- **GIVEN** user wants to update specific date range
- **WHEN** user runs `fdh-cli update --dataset daily --symbols 600519.SH --start-date 2020-01-01 --end-date 2024-12-31`
- **THEN** the command SHALL:
  1. Ignore database existing records for the date range
  2. Fetch data for 2020-01-01 to 2024-12-31
  3. Overwrite any existing data in this range

### Requirement: Intraday Data Overwrite Logic
The system SHALL intelligently handle intraday data updates.

#### Scenario: During trading hours (9:30-15:00)
- **GIVEN** current time is 14:00 on 2024-11-18 (trading day)
- **WHEN** smart download processes symbol with latest record on 2024-11-18
- **THEN** the system SHALL:
  1. Detect latest record is from today during trading hours
  2. Fetch today's data again to update incomplete intraday data
  3. Overwrite the existing today's record with fresh data
  4. Ensure OHLCV reflects current market state

#### Scenario: After trading hours
- **GIVEN** current time is 20:00 on 2024-11-18 (after market close)
- **WHEN** smart download processes symbol with latest record on 2024-11-18
- **THEN** the system SHALL:
  1. Detect latest record is from today after trading hours
  2. Skip today's data (already complete)
  3. Fetch from next trading day (if any)
  4. Ensure no partial day data is fetched

### Requirement: Update Strategy Matrix
The system SHALL automatically select optimal update strategy based on parameters.

#### Strategy Selection Matrix:
| Parameters | Strategy Selected |
|------------|-------------------|
| `--symbols X` (no dates, no --force) | Smart download (incremental if exists, full if new) |
| `--symbols X --force` | Force full update |
| `--symbols X --start-date Y` | Force update from Y to today |
| `--symbols X --start-date Y --end-date Z` | Force update from Y to Z |
| `--trade-date D` | Batch update using trade_date (Tushare only) |
| No `--symbols` (no dates) | Smart download for all symbols |

### Requirement: Dataset Parameter
The `--dataset` parameter SHALL accurately describe data types.

#### Supported Dataset Types:
- **Time-series data**: `daily`, `minute_1`, `minute_5`, `minute_15`, `minute_30`, `minute_60`
- **Indicator data**: `daily_basic`, `adj_factor`
- The `--frequency` parameter is deprecated but still supported for backward compatibility

### Requirement: Progress Display
The system SHALL provide detailed progress information showing update decisions.

#### Scenario: Verbose output for smart download
- **WHEN** user runs `fdh-cli update --dataset daily --symbols 600519.SH --verbose`
- **THEN** the command SHALL display:
  - Symbol being processed
  - Latest record timestamp found (or "new symbol")
  - Strategy selected (smart incremental / smart full / force update)
  - Date range calculated
  - Provider used
  - Number of records fetched
  - Number of records updated/overwritten
