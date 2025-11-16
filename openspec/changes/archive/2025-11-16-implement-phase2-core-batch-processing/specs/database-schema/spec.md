## ADDED Requirements
### Requirement: Asset Basic Information Table
The system SHALL create an asset_basic table to store fundamental information about traded instruments.

#### Scenario: Create asset_basic table
- **WHEN** database is initialized
- **THEN** asset_basic table SHALL be created with columns:
  - symbol (VARCHAR, PRIMARY KEY): Stock code with exchange suffix (e.g., 600519.SH)
  - name (VARCHAR): Security name
  - market (VARCHAR): Market code (SH, SZ, etc.)
  - list_date (DATE): Listing date
  - delist_date (DATE): Delisting date (nullable)
  - status (VARCHAR): Active/inactive status

#### Scenario: Populate asset_basic from data providers
- **WHEN** update command is executed
- **THEN** asset_basic table SHALL be populated/updated with latest instrument list

### Requirement: Daily Market Data Table (symbol_daily)
The system SHALL create a symbol_daily hypertable for storing daily time-series market data.

#### Scenario: Create symbol_daily hypertable
- **WHEN** database initialization runs
- **THEN** symbol_daily table SHALL be created with:
  - Columns: time (TIMESTAMPTZ), symbol (VARCHAR), open, high, low, close, volume, amount, adj_factor
  - Primary key: (symbol, time)
  - Converted to TimescaleDB hypertable partitioned by time
  - Index on (symbol, time DESC) for efficient queries

#### Scenario: Data retention policy
- **WHEN** data is older than retention period
- **THEN** old data SHALL be automatically compressed and moved to archival storage

### Requirement: Minute-Level Market Data Table (symbol_minute)
The system SHALL create a symbol_minute hypertable for storing intraday time-series data.

#### Scenario: Create symbol_minute hypertable
- **WHEN** database is initialized
- **THEN** symbol_minute table SHALL be created with:
  - Columns: time (TIMESTAMPTZ), symbol (VARCHAR), open, high, low, close, volume, amount
  - Primary key: (symbol, time)
  - Converted to TimescaleDB hypertable
  - Index on (symbol, time DESC) for efficient queries
  - Partition interval: 1 day

#### Scenario: Separate minute data types
- **GIVEN** 1-minute and 5-minute data exist
- **WHEN** queries are executed
- **THEN** data SHALL be distinguished by time interval (1m, 5m)

### Requirement: Financial Data Tables
The system SHALL create tables for storing fundamental data.

#### Scenario: Create financial indicator tables
- **WHEN** database is initialized
- **THEN** the following tables SHALL be created:
  - financial_indicator: Time-series financial ratios
  - balance_sheet: Periodic balance sheet data
  - income_statement: Periodic income statement data
  - cash_flow: Periodic cash flow statement data

### Requirement: Database Migration and Versioning
The system SHALL implement database migrations for schema changes.

#### Scenario: Track schema version
- **WHEN** migrations are applied
- **THEN** schema_version table SHALL be updated with current version

#### Scenario: Automatic migration execution
- **WHEN** application starts with outdated database
- **THEN** pending migrations SHALL be automatically applied in order

#### Scenario: Rollback capability
- **GIVEN** a migration causes issues
- **WHEN** rollback is requested
- **THEN** database SHALL be restored to previous version
