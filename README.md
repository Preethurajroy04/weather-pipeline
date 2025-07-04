# Weather Pipeline Project - Handover Documentation

## Deployment & Operations

### Local Development Setup:

```bash
# 1. Setup and Install dependencies
python -m venv .venv && source venv/bin/activate  # On Windows use `.venv\Scripts\activate`
pip install -r requirements.txt

# 2. Run ETL to populate database
python etl.py

# 3. Start API server
python main.py
# API available at: http://localhost:8000
# Documentation at: http://localhost:8000/docs

# 4. Run tests
pytest test_weather_pipeline.py 
```

## Project Overview

This is a complete weather data pipeline that ingests, processes, and serves weather data through a REST API. The system handles large-scale weather station data from text files, processes it into a SQLite database, calculates yearly statistics, and provides a FastAPI-based web service for data access.

## Architecture Overview

```

│    Weather Data     │ -> │ ETL Pipeline  -> │ SQLite Database │ -> │ FastAPI API │
Text Files (wx_data/)          (etl.py)            (db.py)              (main.py)
 

```

## Tech Stack & Design Decisions

### Core Technologies
- **Python 3.8+**: Chosen for excellent data processing capabilities and ecosystem
- **FastAPI**: Modern, high-performance web framework with automatic API documentation
- **SQLite**: Lightweight, embedded database perfect for this scale of data (handles millions of records)
- **Polars**: High-performance DataFrame library chosen over Pandas for better memory efficiency and speed
- **Pydantic**: Data validation and serialization with automatic OpenAPI schema generation
- **Pytest**: Comprehensive testing framework with fixtures and parametrized tests

### Key Design Decisions

#### Why SQLite?
- **Simplicity**: No separate database server required
- **Performance**: Excellent for read-heavy workloads with proper indexing
- **Portability**: Single file database easy to backup and deploy
- **Sufficient Scale**: Handles millions of weather records efficiently

#### Why Polars over Pandas?
- **Memory Efficiency**: ~2-5x less memory usage for large datasets
- **Performance**: Significantly faster for large file processing
- **Lazy Evaluation**: Optimizes query plans automatically


#### Why Batch Processing?
- **Memory Management**: Prevents OOM errors with large files (some files have 10k+ records)
- **Progress Tracking**: Allows monitoring of long-running processes
- **Error Recovery**: Can continue processing if individual batches fail

## Component Details

### 1. Data Modeling (`models/weather.py`)

**Design Philosophy**: Strong typing with automatic validation and API documentation generation.

```python
# Core Models
- WeatherDataResponse: Individual weather records with temperature conversion
- YearlyStatsResponse: Aggregated yearly statistics  
- PaginationInfo: Standardized pagination metadata
- WeatherDataListResponse: Paginated collections with metadata
```

**Key Features**:
- **Unit Conversion**: Raw data (tenths of degrees/mm) automatically converted to human-readable units (°C, cm)
- **Optional Fields**: Handles missing data (-9999 values) as None/null
- **Schema Examples**: Provides sample data for API documentation
- **Type Safety**: Pydantic validates all data at runtime

**Reasoning**: 
- Separation of raw storage format from API response format allows internal optimization while providing clean API
- Automatic OpenAPI documentation generation saves maintenance overhead
- Type safety prevents runtime errors in production

### 2. Data Ingestion (`etl.py`)

**Design Philosophy**: Robust, memory-efficient batch processing with comprehensive error handling.

#### Key Components:

**WeatherDataETL Class**:
- **Batch Processing**: Configurable batch size (default 10,000 records)
- **Memory Streaming**: Processes files without loading entirely into memory
- **Progress Tracking**: Detailed logging and statistics
- **Error Recovery**: Continues processing if individual files/batches fail

**Critical Methods**:
```python
def parse_weather_file(self, file_path: str) -> int:
    # Streams file in batches using Polars read_csv_batched
    # Processes each batch immediately (no accumulation)
    # Returns count of successfully processed records
    
def validate_data(self, df: pl.DataFrame) -> pl.DataFrame:
    # Converts -9999 missing values to None
    # Validates date format (YYYYMMDD)
    # Removes duplicates based on station_id + date
    # Logs validation statistics
```

**Processing Flow**:
1. **File Discovery**: Scans `wx_data/` directory for `.txt` files
2. **Batch Streaming**: Reads files in configurable chunks (10k records default)
3. **Data Validation**: Cleans missing values, validates formats, removes duplicates
4. **Database Insertion**: Uses bulk inserts with duplicate handling (`INSERT OR REPLACE`)
5. **Statistics Calculation**: Aggregates yearly statistics after all files processed

**Error Handling**:
- **File-Level**: Continues processing remaining files if one fails
- **Batch-Level**: Logs errors but continues with next batch
- **Duplicate Handling**: Uses `INSERT OR REPLACE` for idempotent processing
- **Memory Protection**: Streaming prevents OOM errors on large files

**Performance Optimizations**:
- **Polars**: 2-5x faster than Pandas for large file processing
- **Batch Processing**: Configurable batch size balances memory vs. transaction overhead
- **Bulk Inserts**: `executemany()` for efficient database operations
- **Lazy Evaluation**: Polars optimizes query plans automatically

### 3. Database Layer (`database/db.py`)

**Design Philosophy**: Simple, efficient data access layer with proper transaction management.

#### Schema Design:

**weather_data table**:
```sql
CREATE TABLE weather_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,
    date TEXT NOT NULL,  -- YYYYMMDD format for efficient filtering
    max_temp INTEGER,    -- Stored in tenths of degrees (raw format)
    min_temp INTEGER,    -- Null for missing values
    precipitation INTEGER, -- Stored in tenths of mm (raw format)
    UNIQUE(station_id, date)  -- Prevents duplicates
)
```

**yearly_weather_stats table**:
```sql
CREATE TABLE yearly_weather_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    avg_max_temp REAL,      -- Converted to degrees Celsius
    avg_min_temp REAL,      -- Converted to degrees Celsius  
    total_precipitation REAL, -- Converted to centimeters
    UNIQUE(station_id, year)  -- Prevents duplicates
)
```

**Key Design Decisions**:
- **Raw Storage**: Weather data stored in original format (tenths) for precision
- **Converted Aggregates**: Statistics pre-converted to human-readable units
- **Text Dates**: YYYYMMDD format allows efficient string-based filtering
- **Unique Constraints**: Prevents duplicate data during re-processing

#### Critical Methods:

**Data Operations**:
```python
def insert_many_weather_data(self, data_list):
    # Bulk insert with automatic -9999 -> None conversion
    # Uses INSERT OR REPLACE for idempotent processing
    
def calculate_yearly_stats(self):
    # Single SQL query calculates all yearly statistics
    # Handles missing values and unit conversion
    # Uses CASE statements for robust null handling
```

**Statistics Calculation**:
The yearly statistics are calculated using a single, efficient SQL query that:
- Groups by station and year (extracted from YYYYMMDD date)
- Excludes -9999 missing values from calculations
- Converts units (tenths to degrees/cm)
- Handles edge cases (all missing data = null result)

**Connection Management**:
- **Thread Safety**: `check_same_thread=False` for FastAPI compatibility
- **Resource Cleanup**: Proper connection closing in all code paths
- **Error Handling**: Comprehensive SQLite exception handling

### 4. Analysis Layer

**Yearly Statistics Calculation**:
The system calculates comprehensive yearly statistics using SQL aggregations:

```sql
-- Example of the statistics calculation logic
SELECT 
    station_id,
    CAST(substr(date, 1, 4) AS INTEGER) as year,
    ROUND(AVG(CASE WHEN max_temp != -9999 THEN max_temp / 10.0 END), 2) as avg_max_temp,
    ROUND(AVG(CASE WHEN min_temp != -9999 THEN min_temp / 10.0 END), 2) as avg_min_temp,
    ROUND(SUM(CASE WHEN precipitation != -9999 THEN precipitation / 100.0 ELSE 0 END), 2) as total_precipitation
FROM weather_data
GROUP BY station_id, year
```

**Key Features**:
- **Missing Value Handling**: Excludes -9999 values from averages
- **Unit Conversion**: Tenths of degrees → Celsius, tenths of mm → cm
- **Precision Control**: Results rounded to 2 decimal places
- **Null Safety**: Returns NULL when no valid data available

**Performance**: 
- **Single Query**: All statistics calculated in one database operation
- **Indexing**: Efficient grouping on station_id and year
- **Pre-calculation**: Results stored for fast API access

### 5. API Layer (`main.py`)

**Design Philosophy**: RESTful API with automatic documentation, pagination, and comprehensive filtering.

#### Endpoints:

**GET /api/weather**:
- **Purpose**: Retrieve paginated weather data with filtering
- **Filters**: station_id, date, date ranges (start_date/end_date)
- **Pagination**: Configurable page size (max 1000)
- **Response**: Converted units (°C, cm) with pagination metadata

**GET /api/weather/stats**:
- **Purpose**: Retrieve yearly statistics with filtering  
- **Filters**: station_id, year, year ranges (start_year/end_year)
- **Pagination**: Same pagination system as weather data
- **Response**: Pre-calculated statistics in human-readable units

**Supporting Endpoints**:
- **GET /**: API information and available endpoints
- **GET /health**: Health check for monitoring
- **GET /docs**: Automatic OpenAPI documentation (FastAPI built-in)

#### Key Features:

**Pagination System**:
```python
class PaginationInfo(BaseModel):
    page: int              # Current page (1-based)
    page_size: int         # Items per page  
    total_items: int       # Total matching records
    total_pages: int       # Total pages available
    has_next: bool         # Navigation hints
    has_previous: bool
```

**Dynamic Filtering**:
- **SQL Building**: Dynamically constructs WHERE clauses based on provided filters
- **Parameter Binding**: Prevents SQL injection through parameterized queries
- **Flexible Ranges**: Supports both exact matches and range queries


### 6. Unit Testing (`test_weather_pipeline.py`)

**Design Philosophy**: Comprehensive test coverage using pytest fixtures with isolated test environments.

#### Test Structure:

**Fixtures**:
```python
@pytest.fixture
def temp_db():
    # Creates isolated temporary database for each test
    # Ensures no test interference
    
@pytest.fixture  
def sample_weather_data():
    # Provides consistent test data
    # Includes edge cases (missing values, different stations)
```

**Test Categories**:

1. **Database Tests** (`test_database_*`):
   - Connection management
   - Table creation
   - Data insertion (single and bulk)
   - Yearly statistics calculation
   - Query functionality

2. **ETL Tests** (`test_etl_*`):
   - File parsing and validation
   - Batch processing
   - Data conversion (-9999 handling)
   - Error recovery

3. **API Tests** (`test_api_*`):
   - Endpoint functionality
   - Filtering and pagination
   - Data conversion
   - Error responses

4. **Model Tests** (`test_models_*`):
   - Pydantic validation
   - Unit conversion
   - Response serialization

