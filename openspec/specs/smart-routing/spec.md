# smart-routing Specification

## Purpose
TBD - created by archiving change implement-phase2-core-batch-processing. Update Purpose after archive.
## Requirements
### Requirement: Sources Configuration (sources.yml)
The system SHALL support a YAML configuration file (sources.yml) that defines data providers, their capabilities, and routing strategies.

#### Scenario: Load sources configuration
- **WHEN** the system starts
- **THEN** it SHALL load sources.yml from configured path and parse provider definitions

#### Scenario: Define multiple providers
- **GIVEN** sources.yml contains tushare and xtquant provider definitions
- **WHEN** system loads the configuration
- **THEN** it SHALL register both providers with their respective API endpoints and capabilities

### Requirement: Smart Data Source Router
The system SHALL provide an intelligent router that automatically selects the best data source based on configuration rules. The router SHALL now integrate with SDK data access layer for data source provider selection and availability checking.

#### Scenario: SDK integration for data source routing
- **GIVEN** FinanceDataHub SDK is initialized with SmartRouter
- **WHEN** user calls `fdh.get_daily(['600519.SH'], '2020-01-01', '2024-12-31')`
- **THEN** system SHALL:
  - Use SmartRouter to determine optimal data source provider
  - Check provider availability and health
  - Log data source selection decision with timestamp and reason
  - Support failover if primary provider fails
  - Recommend data update if data is stale

#### Scenario: SmartRouter availability check
- **GIVEN** SmartRouter is integrated with SDK
- **WHEN** user queries data that may need updating
- **THEN** system SHALL:
  - Check with SmartRouter if data source is available
  - Recommend update if data is stale
  - Proceed with query from PostgreSQL if data is current
  - Log all routing decisions

### Requirement: Dynamic Provider Selection
The router SHALL support dynamic provider selection based on runtime conditions.

#### Scenario: Load balancing
- **GIVEN** multiple providers are available for the same data type
- **WHEN** system needs to fetch data
- **THEN** the router SHALL distribute requests across providers based on configured weights

#### Scenario: Provider health check
- **GIVEN** a provider becomes unhealthy
- **WHEN** router evaluates available providers
- **THEN** it SHALL exclude unhealthy providers from selection

#### Scenario: Runtime configuration reload
- **GIVEN** sources.yml is updated
- **WHEN** system receives reload signal
- **THEN** it SHALL reinitialize all providers with new configuration

### Requirement: Routing Statistics and Monitoring
The system SHALL track routing decisions and provider performance metrics. The router SHALL track data source provider metrics for SDK integration.

#### Scenario: Track provider usage
- **WHEN** router selects a data source provider
- **THEN** it SHALL record:
  - Provider type used for the query
  - Query parameters (symbols, time range, data type)
  - Selection rationale
  - Query execution time

#### Scenario: Provider performance metrics
- **WHEN** query completes against a provider
- **THEN** it SHALL update metrics for:
  - Average query latency by provider
  - Data volume processed by provider
  - Success/failure rates by provider
  - Availability metrics per provider

