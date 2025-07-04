"""
Comprehensive unit tests for the Weather Pipeline using pytest fixtures.
Tests core functionalities: database operations, ETL processes, data conversions, 
API endpoints, and filtering options.
"""

import pytest
import tempfile
import os
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
import polars as pl

# Import modules to test
from database.db import Database
from etl import WeatherDataETL
from main import app, convert_weather_data_row, convert_stats_row, get_database
from models.weather import WeatherDataResponse, YearlyStatsResponse



# ========== PYTEST FIXTURES ==========

@pytest.fixture
def temp_db():
    """Create a temporary database for testing"""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_file.close()
    db_path = temp_file.name
    
    db = Database(db_path)
    db.connect()
    db.create_tables()
    
    yield db
    
    # Cleanup
    db.close()
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def sample_weather_data():
    """Sample weather data for testing"""
    return [
        ("USC00110072", "19850101", 50, -11, 25),   # 5.0°C, -1.1°C, 0.25cm
        ("USC00110072", "19850102", -9999, 22, 0),  # Missing max_temp
        ("USC00110072", "19850103", 78, -9999, -9999), # Missing min_temp and precipitation
        ("USC00110187", "19850101", 120, 45, 100),  # Different station
        ("USC00110072", "19851201", 30, -50, 150),  # Different date (December)
    ]


@pytest.fixture
def sample_yearly_stats():
    """Sample yearly statistics for testing"""
    return [
        ("USC00110072", 1985, 15.2, 3.8, 45.67),
        ("USC00110187", 1985, 12.5, 1.2, 38.90),
        ("USC00110072", 1986, 16.8, 4.1, 52.33),
    ]


@pytest.fixture
def test_client():
    """FastAPI test client"""
    return TestClient(app)


@pytest.fixture
def temp_weather_file():
    """Create a temporary weather data file for ETL testing"""
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
    
    # Write sample weather data in the expected format
    sample_data = [
        "19850101\t50\t-11\t25",
        "19850102\t-9999\t22\t0", 
        "19850103\t78\t-9999\t-9999",
        "19850104\t65\t15\t50",
    ]
    
    for line in sample_data:
        temp_file.write(line + '\n')
    
    temp_file.close()
    
    yield temp_file.name
    
    # Cleanup
    if os.path.exists(temp_file.name):
        os.unlink(temp_file.name)


# ========== DATABASE TESTS ==========

class TestDatabase:
    """Test database operations"""
    
    def test_database_connection(self, temp_db):
        """Test database connection is successful"""
        assert temp_db.connection is not None
        assert temp_db.cursor is not None
    
    def test_table_creation(self, temp_db):
        """Test that database tables are created properly"""
        # Check if weather_data table exists
        result = temp_db.query_data(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='weather_data'"
        )
        assert len(result) == 1
        assert result[0][0] == 'weather_data'
        
        # Check if yearly_weather_stats table exists
        result = temp_db.query_data(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='yearly_weather_stats'"
        )
        assert len(result) == 1
        assert result[0][0] == 'yearly_weather_stats'
    
    def test_insert_single_record(self, temp_db):
        """Test inserting a single weather record"""
        success = temp_db.insert_weather_data("USC00110072", "19850101", 50, -11, 25)
        assert success is True
        
        # Verify the record was inserted
        result = temp_db.query_data(
            "SELECT station_id, date, max_temp, min_temp, precipitation FROM weather_data"
        )
        assert len(result) == 1
        assert result[0] == ("USC00110072", "19850101", 50, -11, 25)
    
    def test_handle_duplicates(self, temp_db, sample_weather_data):
        """Test that duplicate records are handled properly (replaced)"""
        # Insert initial data
        temp_db.insert_many_weather_data(sample_weather_data)
        
        # Insert duplicate with different values
        duplicate_data = [("USC00110072", "19850101", 60, -5, 30)]  # Same station/date, different values
        temp_db.insert_many_weather_data(duplicate_data)
        
        # Check that only one record exists and it has the new values
        result = temp_db.query_data(
            "SELECT max_temp, min_temp, precipitation FROM weather_data WHERE station_id='USC00110072' AND date='19850101'"
        )
        assert len(result) == 1
        assert result[0] == (60, -5, 30)
    
    def test_insert_many_records(self, temp_db, sample_weather_data):
        """Test bulk insertion of weather records"""
        success = temp_db.insert_many_weather_data(sample_weather_data)
        assert success is True
        
        # Verify all records were inserted
        result = temp_db.query_data("SELECT COUNT(*) FROM weather_data")
        assert result[0][0] == len(sample_weather_data)
    
    def test_missing_values_handling(self, temp_db):
        """Test that missing values (-9999) are stored as NULL in database"""
        # Insert record with missing values
        temp_db.insert_weather_data("USC00110072", "19850101", -9999, -9999, -9999)
        
        result = temp_db.query_data(
            "SELECT max_temp, min_temp, precipitation FROM weather_data"
        )
        # -9999 values should be stored as NULL in the database
        assert result[0] == (None, None, None)


# ========== ETL TESTS ==========

class TestETL:
    """Test ETL processes"""
    
    def test_data_parsing(self, temp_weather_file, temp_db):
        """Test parsing weather data from file"""
        etl = WeatherDataETL(data_dir="", batch_size=100, db_path=temp_db.db_path)
        etl.db = temp_db  # Use test database
        
        # Parse the test file
        records_processed = etl.parse_weather_file(temp_weather_file)
        
        assert records_processed > 0
        
        # Verify data was inserted
        result = temp_db.query_data("SELECT COUNT(*) FROM weather_data")
        assert result[0][0] == records_processed
    
    def test_data_validation(self):
        """Test data validation and cleaning"""
        # Create sample DataFrame with issues
        df = pl.DataFrame({
            'station_id': ['USC00110072', 'USC00110072', 'USC00110072', 'USC00110072'],
            'date': ['19850101', '1985010', '19850102', '19850101'],  # One invalid date
            'max_temp': [50, -9999, 78, 50],  # One missing value, one duplicate
            'min_temp': [-11, 22, -9999, -11], 
            'precipitation': [25, 0, -9999, 25]
        })
        
        etl = WeatherDataETL()
        validated_df = etl.validate_data(df)
        
        # Should remove invalid date and duplicate
        assert len(validated_df) == 2
        
        # Check that -9999 values are converted to None
        max_temps = validated_df['max_temp'].to_list()
        # After validation and removing duplicates, we should have the record with max_temp=78 (no None values in the remaining records)
        assert 78 in max_temps


# ========== DATA CONVERSION TESTS ==========

class TestDataConversion:
    """Test data conversion functions"""
    
    def test_weather_data_conversion(self):
        """Test conversion from database row to WeatherDataResponse"""
        # Raw data from database (temperatures in tenths of degrees, precipitation in tenths of mm)
        row = ("USC00110072", "19850101", 50, -11, 25)
        
        result = convert_weather_data_row(row)
        
        assert isinstance(result, WeatherDataResponse)
        assert result.station_id == "USC00110072"
        assert result.date == "19850101"
        assert result.max_temp == 5.0  # 50/10
        assert result.min_temp == -1.1  # -11/10
        assert result.precipitation == 0.25  # 25/100
    
    def test_weather_data_conversion_with_nulls(self):
        """Test conversion with None values"""
        row = ("USC00110072", "19850101", None, None, None)
        
        result = convert_weather_data_row(row)
        
        assert result.max_temp is None
        assert result.min_temp is None
        assert result.precipitation is None
    
    def test_stats_conversion(self):
        """Test conversion from database row to YearlyStatsResponse"""
        row = ("USC00110072", 1985, 15.2, 3.8, 45.67)
        
        result = convert_stats_row(row)
        
        assert isinstance(result, YearlyStatsResponse)
        assert result.station_id == "USC00110072"
        assert result.year == 1985
        assert result.avg_max_temp == 15.2
        assert result.avg_min_temp == 3.8
        assert result.total_precipitation == 45.67


# ========== YEARLY STATS TESTS ==========

class TestYearlyStats:
    """Test yearly statistics calculation"""
    
    def test_yearly_stats_calculation(self, temp_db, sample_weather_data):
        """Test calculation of yearly statistics"""
        # Insert sample data
        temp_db.insert_many_weather_data(sample_weather_data)
        
        # Calculate yearly stats
        success = temp_db.calculate_yearly_stats()
        assert success is True
        
        # Verify stats were calculated
        result = temp_db.query_data("SELECT COUNT(*) FROM yearly_weather_stats")
        assert result[0][0] > 0
        
        # Check specific calculation for USC00110072, 1985
        stats = temp_db.query_data(
            "SELECT avg_max_temp, avg_min_temp, total_precipitation FROM yearly_weather_stats WHERE station_id='USC00110072' AND year=1985"
        )
        assert len(stats) == 1
        
        # The calculation should exclude -9999 value
        avg_max, avg_min, total_precip = stats[0]
        assert avg_max is not None  # Should have valid average
        assert avg_min is not None  # Should have valid average
        assert total_precip is not None  # Should have valid total
    
    def test_yearly_stats_insertion(self, temp_db, sample_yearly_stats):
        """Test direct insertion of yearly statistics"""
        success = temp_db.insert_many_yearly_stats(sample_yearly_stats)
        assert success is True
        
        # Verify insertion
        result = temp_db.query_data("SELECT COUNT(*) FROM yearly_weather_stats")
        assert result[0][0] == len(sample_yearly_stats)


# ========== API TESTS ==========

class TestAPI:
    """Test API endpoints"""
    
    def test_weather_endpoint_basic(self, test_client):
        """Test basic weather data endpoint"""
        # Mock the database dependency directly in the app
        def mock_get_database():
            mock_db = MagicMock()
            mock_db.query_data.side_effect = [
                [(1,)],  # Count query
                [("USC00110072", "19850101", 50, -11, 25)]  # Data query
            ]
            yield mock_db
        
        # Override the dependency
        from main import app
        app.dependency_overrides[get_database] = mock_get_database
        
        try:
            response = test_client.get("/api/weather")
            
            assert response.status_code == 200
            data = response.json()
            assert "data" in data
            assert "pagination" in data
            assert len(data["data"]) == 1
            assert data["data"][0]["station_id"] == "USC00110072"
        finally:
            # Clean up the override
            app.dependency_overrides.clear()
    
    def test_weather_endpoint_filtering(self, test_client):
        """Test weather endpoint with filtering"""
        def mock_get_database():
            mock_db = MagicMock()
            mock_db.query_data.side_effect = [
                [(3,)],  # Count query
                [("USC00110072", "19850101", 50, -11, 25),
                 ("USC00110072", "19850102", 60, -5, 30),
                 ("USC00110072", "19850103", 70, 0, 35)]
            ]
            yield mock_db
        
        from main import app
        app.dependency_overrides[get_database] = mock_get_database
        
        try:
            # Test station_id filter
            response = test_client.get("/api/weather?station_id=USC00110072")
            assert response.status_code == 200
            
            # Test date range filter
            response = test_client.get("/api/weather?start_date=19850101&end_date=19850103")
            assert response.status_code == 200
            
            # Test specific date filter
            response = test_client.get("/api/weather?date=19850101")
            assert response.status_code == 200
        finally:
            app.dependency_overrides.clear()
    
    def test_weather_endpoint_pagination(self, test_client):
        """Test weather endpoint pagination"""
        def mock_get_database():
            mock_db = MagicMock()
            mock_db.query_data.side_effect = [
                [(250,)],  # Count query - total 250 records
                [("USC00110072", "19850101", 50, -11, 25)] * 50  # 50 records per page
            ]
            yield mock_db
        
        from main import app
        app.dependency_overrides[get_database] = mock_get_database
        
        try:
            response = test_client.get("/api/weather?page=2&page_size=50")
            
            assert response.status_code == 200
            data = response.json()
            assert data["pagination"]["page"] == 2
            assert data["pagination"]["page_size"] == 50
            assert data["pagination"]["total_items"] == 250
            assert data["pagination"]["total_pages"] == 5
            assert data["pagination"]["has_next"] is True
            assert data["pagination"]["has_previous"] is True
        finally:
            app.dependency_overrides.clear()
    
    def test_stats_endpoint_basic(self, test_client):
        """Test basic yearly stats endpoint"""
        def mock_get_database():
            mock_db = MagicMock()
            mock_db.query_data.side_effect = [
                [(2,)],  # Count query
                [("USC00110072", 1985, 15.2, 3.8, 45.67),
                 ("USC00110187", 1985, 12.5, 1.2, 38.90)]
            ]
            yield mock_db
        
        from main import app
        app.dependency_overrides[get_database] = mock_get_database
        
        try:
            response = test_client.get("/api/weather/stats")
            
            assert response.status_code == 200
            data = response.json()
            assert "data" in data
            assert "pagination" in data
            assert len(data["data"]) == 2
            assert data["data"][0]["year"] == 1985
        finally:
            app.dependency_overrides.clear()
    
    def test_stats_endpoint_filtering(self, test_client):
        """Test yearly stats endpoint with different filtering options"""
        def mock_get_database():
            mock_db = MagicMock()
            mock_db.query_data.side_effect = [
                [(1,)],  # Count query
                [("USC00110072", 1985, 15.2, 3.8, 45.67)]
            ]
            yield mock_db
        
        from main import app
        app.dependency_overrides[get_database] = mock_get_database
        
        try:   
            # Test year filter
            response = test_client.get("/api/weather/stats?year=1985")
            assert response.status_code == 200
            
        finally:
            app.dependency_overrides.clear()
    
    def test_root_endpoint(self, test_client):
        """Test root endpoint"""
        response = test_client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "endpoints" in data


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
