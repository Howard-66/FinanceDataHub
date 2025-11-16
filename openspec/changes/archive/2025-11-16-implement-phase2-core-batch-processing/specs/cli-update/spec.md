## ADDED Requirements
### Requirement: fdh-cli update Command Enhancement
The `fdh-cli update` command SHALL be enhanced to support complete data synchronization from sources to database.

#### Scenario: Execute full update
- **WHEN** user runs `fdh-cli update --asset-class stock --frequency daily`
- **THEN** the command SHALL:
  1. Load configuration from sources.yml
  2. Initialize data providers (Tushare, XTQuant)
  3. Use smart router to select appropriate provider
  4. Fetch data from provider
  5. Validate and standardize data format
  6. Insert/update data in TimescaleDB
  7. Display progress and results

#### Scenario: Incremental update mode
- **WHEN** user runs `fdh-cli update --mode incremental`
- **THEN** the command SHALL:
  1. Query database for latest timestamp per symbol
  2. Fetch only newer data from providers
  3. Append new records to database
  4. Skip existing data

#### Scenario: Full refresh mode
- **WHEN** user runs `fdh-cli update --mode full`
- **THEN** the command SHALL:
  1. Delete all existing data for specified symbols/frequency
  2. Fetch complete dataset from providers
  3. Insert fresh data into database

#### Scenario: Update specific symbols
- **WHEN** user runs `fdh-cli update --symbols 600519.SH,000858.SZ`
- **THEN** the command SHALL only update data for specified symbols

### Requirement: Progress Display and User Feedback
The system SHALL provide clear progress information during update operations.

#### Scenario: Show update progress
- **WHEN** update is in progress
- **THEN** the command SHALL display:
  - Current symbol being processed
  - Progress percentage
  - Number of records fetched/inserted
  - Estimated time remaining

#### Scenario: Error handling with details
- **WHEN** an error occurs during update
- **THEN** the command SHALL display:
  - Clear error message
  - Symbol that caused error
  - Provider that failed
  - Suggested action (retry, check config, etc.)

### Requirement: Provider Integration
The update command SHALL integrate with all configured data providers.

#### Scenario: Use smart router selection
- **WHEN** update command starts
- **THEN** it SHALL:
  1. Query smart router for each symbol
  2. Get recommended provider from router
  3. Fetch data from selected provider
  4. Handle provider-specific errors gracefully

#### Scenario: Multiple data frequencies
- **WHEN** user runs `fdh-cli update --frequency minute_1`
- **THEN** the command SHALL:
  1. Select providers that support minute_1 data
  2. Fetch data at 1-minute intervals
  3. Store in symbol_minute table

### Requirement: Database Transaction Management
The system SHALL ensure data consistency using proper database transactions.

#### Scenario: Batch transaction for performance
- **WHEN** inserting multiple records
- **THEN** the system SHALL use batch inserts within transactions
  - Default batch size: 1000 records
  - Configurable via ETL batch_size setting

#### Scenario: Rollback on failure
- **WHEN** error occurs during data insertion
- **THEN** the transaction SHALL be rolled back
- **AND** partial data SHALL NOT persist in database

#### Scenario: Idempotent operations
- **WHEN** same update is run multiple times
- **THEN** database SHALL remain in same consistent state
- **AND** no duplicate records SHALL be created
