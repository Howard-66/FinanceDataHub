# data-providers Specification

## Purpose
TBD - created by archiving change implement-phase2-core-batch-processing. Update Purpose after archive.
## Requirements
### Requirement: TushareProvider Data Fetcher
The system SHALL provide a TushareProvider class that fetches financial data directly from Tushare API and returns standardized pandas DataFrame.

#### Scenario: Fetch daily stock data
- **GIVEN** Tushare token is configured in environment
- **WHEN** provider fetches daily data for stock 600519.SH from 2024-01-01 to 2024-12-31
- **THEN** the provider SHALL return a DataFrame with columns: time, symbol, open, high, low, close, volume, amount, adj_factor

#### Scenario: Fetch minute-level data
- **GIVEN** Tushare token is configured
- **WHEN** provider fetches 1-minute data for stock 000001.SZ
- **THEN** the provider SHALL return a DataFrame with standardized column names and datetime index

#### Scenario: Handle API rate limiting
- **GIVEN** Tushare API reaches rate limit
- **WHEN** provider attempts to fetch data
- **THEN** the provider SHALL implement exponential backoff retry mechanism with maximum 3 retries

### Requirement: XTQuantProvider HTTP Client
The system SHALL provide an XTQuantProvider class that communicates with xtquant_helper microservice via HTTP API.

#### Scenario: Fetch market data via API
- **GIVEN** xtquant_helper microservice is running at http://localhost:8100
- **WHEN** provider requests market data for stock 600000.SH
- **THEN** the provider SHALL call /get_market_data endpoint and convert response to standardized DataFrame

#### Scenario: Fetch local cached data
- **GIVEN** xtquant_helper microservice is available
- **WHEN** provider requests local data for multiple stocks
- **THEN** the provider SHALL call /get_local_data endpoint with stock_list parameter

#### Scenario: Handle service unavailable
- **GIVEN** xtquant_helper microservice is not responding
- **WHEN** provider attempts to fetch data
- **THEN** the provider SHALL raise a clear error indicating service is unavailable

### Requirement: Standardized Data Format
All providers SHALL return data in a standardized format with consistent column names and data types.

#### Scenario: Column name standardization
- **WHEN** provider returns stock market data
- **THEN** the data SHALL have columns: time, symbol, open, high, low, close, volume, amount, adj_factor (if available)

#### Scenario: Datetime format
- **WHEN** provider returns time-series data
- **THEN** the time column SHALL be pandas datetime64[ns] type with UTC timezone or timezone-naive

#### Scenario: Numeric type consistency
- **WHEN** provider returns price and volume data
- **THEN** price columns SHALL be float64 and volume SHALL be int64

### Requirement: Provider Error Handling
The system SHALL implement comprehensive error handling for all data providers.

#### Scenario: Invalid stock code
- **GIVEN** an invalid stock code is provided
- **WHEN** provider attempts to fetch data
- **THEN** the provider SHALL raise ProviderError with detailed message

#### Scenario: Network timeout
- **GIVEN** network connection times out
- **WHEN** provider is fetching data
- **THEN** the provider SHALL retry up to 3 times with exponential backoff

#### Scenario: Empty data response
- **GIVEN** data source returns empty dataset
- **WHEN** provider processes the response
- **THEN** the provider SHALL return empty DataFrame with correct schema

### Requirement: Incremental Data Fetching
All providers SHALL support incremental data fetching to enable smart download mode.

#### Scenario: Fetch with None date range (full download)
- **GIVEN** provider is configured with valid credentials
- **WHEN** get_daily_data is called with start_date=None, end_date=None
- **THEN** the provider SHALL fetch complete historical data from the beginning

#### Scenario: Fetch with calculated date range (incremental)
- **GIVEN** provider has last record date from database
- **WHEN** get_daily_data is called with start_date after last record
- **THEN** the provider SHALL fetch only data from specified start_date onwards

#### Scenario: Trade date batch fetching
- **GIVEN** provider supports trade_date parameter (Tushare)
- **WHEN** get_daily_data is called with trade_date="2024-11-18"
- **THEN** the provider SHALL batch fetch all stocks for that trading date

#### Scenario: Auto-batching for large datasets
- **GIVEN** provider receives more than 6000 records (Tushare limit)
- **WHEN** fetching daily data
- **THEN** the provider SHALL automatically batch requests adjusting end_date backwards

#### Scenario: Provider method signature consistency
- **GIVEN** any data provider (Tushare, XTQuant)
- **WHEN** calling data fetching methods
- **THEN** all providers SHALL support these parameters:
  - symbol: Optional[str] (None for batch/all)
  - start_date: Optional[str] (None for smart/fetch all)
  - end_date: Optional[str] (None for fetch to latest)
  - trade_date: Optional[str] (None for regular mode)

