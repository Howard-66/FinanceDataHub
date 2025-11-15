## ADDED Requirements
### Requirement: fdh-cli Command Structure
The system SHALL provide a command-line interface tool called `fdh-cli` using the Typer framework with a modular command structure.

#### Scenario: Display help information
- **WHEN** user runs `fdh-cli --help`
- **THEN** the CLI SHALL display available commands and their descriptions

#### Scenario: Display subcommand help
- **WHEN** user runs `fdh-cli update --help`
- **THEN** the CLI SHALL display help for the update command including available options

### Requirement: update Command
The `fdh-cli update` command SHALL trigger data synchronization from configured data sources to the database.

#### Scenario: Execute update with default settings
- **GIVEN** data sources are properly configured
- **WHEN** user runs `fdh-cli update`
- **THEN** the command SHALL attempt to fetch data from the configured sources

#### Scenario: Update with asset class option
- **WHEN** user runs `fdh-cli update --asset-class stock`
- **THEN** the command SHALL only update stock-related data

#### Scenario: Update with frequency option
- **WHEN** user runs `fdh-cli update --frequency daily`
- **THEN** the command SHALL only update daily frequency data

### Requirement: etl Command
The `fdh-cli etl` command SHALL execute the Extract, Transform, Load process to sync data from PostgreSQL to analytical storage (Parquet+DuckDB).

#### Scenario: Execute full ETL
- **WHEN** user runs `fdh-cli etl --from-date 2024-01-01`
- **THEN** the command SHALL extract data from the database, transform it, and load it to Parquet files

### Requirement: status Command
The `fdh-cli status` command SHALL display data completeness and health information about the system.

#### Scenario: Display system status
- **WHEN** user runs `fdh-cli status`
- **THEN** the command SHALL display information about database connectivity, data freshness, and service health

#### Scenario: Display detailed status
- **WHEN** user runs `fdh-cli status --verbose`
- **THEN** the command SHALL display detailed metrics including record counts and last update timestamps

### Requirement: Configuration Integration
The CLI tool SHALL automatically load and validate configuration on startup, failing fast with clear error messages if configuration is invalid.

#### Scenario: CLI startup with valid configuration
- **GIVEN** all required environment variables are set
- **WHEN** any `fdh-cli` command is executed
- **THEN** the command SHALL start successfully

#### Scenario: CLI startup with invalid configuration
- **GIVEN** required environment variables are missing
- **WHEN** any `fdh-cli` command is executed
- **THEN** the command SHALL display an error message and exit with a non-zero code
