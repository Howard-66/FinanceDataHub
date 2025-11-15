## ADDED Requirements
### Requirement: Pydantic-Based Configuration Management
The system SHALL provide a centralized, type-safe configuration management module using Pydantic that loads settings from environment variables and `.env` files.

#### Scenario: Load configuration from environment variables
- **GIVEN** the application is started
- **WHEN** environment variables are set for database and Redis connections
- **THEN** the config module SHALL parse and validate these values using Pydantic models

#### Scenario: Configuration validation
- **GIVEN** invalid configuration values are provided
- **WHEN** the application attempts to load the configuration
- **THEN** Pydantic SHALL raise validation errors with clear messages about missing or invalid fields

#### Scenario: Support .env file loading
- **GIVEN** a `.env` file exists in the project root
- **WHEN** the configuration is loaded
- **THEN** the module SHALL automatically load variables from the `.env` file and merge them with environment variables

### Requirement: Environment Variable Structure
The configuration module SHALL support the following environment variables:
- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string
- `TUSHARE_TOKEN`: Tushare API authentication token
- `XTQUANT_API_URL`: XTQuant helper microservice URL
- `SOURCES_CONFIG_PATH`: Path to sources.yml configuration file

#### Scenario: Database connection configuration
- **GIVEN** `DATABASE_URL` is set to a valid PostgreSQL connection string
- **WHEN** the database client is initialized
- **THEN** the connection SHALL be established using the provided URL

#### Scenario: Data source API configuration
- **GIVEN** `TUSHARE_TOKEN` and `XTQUANT_API_URL` are configured
- **WHEN** data providers are initialized
- **THEN** providers SHALL use these credentials for authentication

### Requirement: Configuration Access Interface
The system SHALL provide a singleton configuration instance that can be imported and used throughout the application.

#### Scenario: Import and use configuration
- **GIVEN** the config module is installed
- **WHEN** other modules import `from config import settings`
- **THEN** they SHALL receive the validated configuration instance
