from fastapi import FastAPI, HTTPException, Depends, Query
from typing import Optional
import math
from database.db import Database
from models.weather import (
    WeatherDataListResponse, 
    YearlyStatsListResponse,
    WeatherDataResponse,
    YearlyStatsResponse,
    PaginationInfo,
)

app = FastAPI(
    title="Weather Data API",
    description="API for accessing weather data and yearly statistics",
    version="1.0.0"
)

# Database dependency
def get_database():
    """Database dependency for FastAPI"""
    db = Database()
    if not db.connect():
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        yield db
    finally:
        db.close()


def convert_weather_data_row(row) -> WeatherDataResponse:
    """Convert database row to WeatherDataResponse model"""
    station_id, date, max_temp, min_temp, precipitation = row
    
    # Convert temperatures from tenths of degrees to degrees Celsius
    max_temp_celsius = max_temp / 10.0 if max_temp is not None else None
    min_temp_celsius = min_temp / 10.0 if min_temp is not None else None
    
    # Convert precipitation from tenths of mm to cm
    precipitation_cm = precipitation / 100.0 if precipitation is not None else None
    
    return WeatherDataResponse(
        station_id=station_id,
        date=date,
        max_temp=max_temp_celsius,
        min_temp=min_temp_celsius,
        precipitation=precipitation_cm
    )


def convert_stats_row(row) -> YearlyStatsResponse:
    """Convert database row to YearlyStatsResponse model"""
    station_id, year, avg_max_temp, avg_min_temp, total_precipitation = row
    
    return YearlyStatsResponse(
        station_id=station_id,
        year=year,
        avg_max_temp=avg_max_temp,
        avg_min_temp=avg_min_temp,
        total_precipitation=total_precipitation
    )


@app.get("/api/weather", response_model=WeatherDataListResponse)
async def get_weather_data(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    date: Optional[str] = Query(None, description="Filter by specific date (YYYYMMDD)"),
    start_date: Optional[str] = Query(None, description="Filter by start date (YYYYMMDD)"),
    end_date: Optional[str] = Query(None, description="Filter by end date (YYYYMMDD)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of items per page"),
    db: Database = Depends(get_database)
):
    """
    Get weather data with optional filtering and pagination.
    
    - **station_id**: Filter by weather station ID
    - **date**: Filter by specific date (YYYYMMDD format)
    - **start_date**: Filter by start date (YYYYMMDD format)
    - **end_date**: Filter by end date (YYYYMMDD format)
    - **page**: Page number (starts from 1)
    - **page_size**: Number of records per page (max 1000)
    """
    try:
        # Build the query
        base_query = "SELECT station_id, date, max_temp, min_temp, precipitation FROM weather_data"
        count_query = "SELECT COUNT(*) FROM weather_data"
        
        conditions = []
        params = []
        
        # Add filters
        if station_id:
            conditions.append("station_id = ?")
            params.append(station_id)
            
        if date:
            conditions.append("date = ?")
            params.append(date)
        elif start_date or end_date:
            if start_date:
                conditions.append("date >= ?")
                params.append(start_date)
            if end_date:
                conditions.append("date <= ?")
                params.append(end_date)
        
        # Add WHERE clause if there are conditions
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            base_query += where_clause
            count_query += where_clause
        
        # Get total count
        total_count_result = db.query_data(count_query, params)
        total_items = total_count_result[0][0] if total_count_result else 0
        
        # Calculate pagination
        total_pages = math.ceil(total_items / page_size) if total_items > 0 else 0
        offset = (page - 1) * page_size
        
        # Add pagination
        paginated_query = base_query + " ORDER BY station_id, date LIMIT ? OFFSET ?"
        paginated_params = params + [page_size, offset]
        
        # Execute query
        results = db.query_data(paginated_query, paginated_params)
        
        if results is None:
            raise HTTPException(status_code=500, detail="Database query failed")
        
        # Convert results to response models
        weather_data = [convert_weather_data_row(row) for row in results]
        
        # Create pagination info
        pagination = PaginationInfo(
            page=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        )
        
        return WeatherDataListResponse(
            data=weather_data,
            pagination=pagination
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/api/weather/stats", response_model=YearlyStatsListResponse)
async def get_weather_stats(
    station_id: Optional[str] = Query(None, description="Filter by station ID"),
    year: Optional[int] = Query(None, description="Filter by specific year"),
    start_year: Optional[int] = Query(None, description="Filter by start year"),
    end_year: Optional[int] = Query(None, description="Filter by end year"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of items per page"),
    db: Database = Depends(get_database)
):
    """
    Get yearly weather statistics with optional filtering and pagination.
    
    - **station_id**: Filter by weather station ID
    - **year**: Filter by specific year
    - **start_year**: Filter by start year
    - **end_year**: Filter by end year
    - **page**: Page number (starts from 1)
    - **page_size**: Number of records per page (max 1000)
    """
    try:
        # Build the query
        base_query = "SELECT station_id, year, avg_max_temp, avg_min_temp, total_precipitation FROM yearly_weather_stats"
        count_query = "SELECT COUNT(*) FROM yearly_weather_stats"
        
        conditions = []
        params = []
        
        # Add filters
        if station_id:
            conditions.append("station_id = ?")
            params.append(station_id)
            
        if year:
            conditions.append("year = ?")
            params.append(year)
        elif start_year or end_year:
            if start_year:
                conditions.append("year >= ?")
                params.append(start_year)
            if end_year:
                conditions.append("year <= ?")
                params.append(end_year)
        
        # Add WHERE clause if there are conditions
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            base_query += where_clause
            count_query += where_clause
        
        # Get total count
        total_count_result = db.query_data(count_query, params)
        total_items = total_count_result[0][0] if total_count_result else 0
        
        # Calculate pagination
        total_pages = math.ceil(total_items / page_size) if total_items > 0 else 0
        offset = (page - 1) * page_size
        
        # Add pagination
        paginated_query = base_query + " ORDER BY station_id, year LIMIT ? OFFSET ?"
        paginated_params = params + [page_size, offset]
        
        # Execute query
        results = db.query_data(paginated_query, paginated_params)
        
        if results is None:
            raise HTTPException(status_code=500, detail="Database query failed")
        
        # Convert results to response models
        stats_data = [convert_stats_row(row) for row in results]
        
        # Create pagination info
        pagination = PaginationInfo(
            page=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        )
        
        return YearlyStatsListResponse(
            data=stats_data,
            pagination=pagination
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Weather Data API",
        "version": "1.0.0",
        "endpoints": {
            "weather_data": "/api/weather",
            "weather_stats": "/api/weather/stats",
            "docs": "/docs"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
