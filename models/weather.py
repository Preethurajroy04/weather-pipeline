from pydantic import BaseModel, Field
from typing import Optional, List


class WeatherDataResponse(BaseModel):
    """Response model for individual weather data records"""
    station_id: str = Field(..., description="Weather station identifier")
    date: str = Field(..., description="Date in YYYYMMDD format")
    max_temp: Optional[float] = Field(None, description="Maximum temperature in degrees Celsius")
    min_temp: Optional[float] = Field(None, description="Minimum temperature in degrees Celsius") 
    precipitation: Optional[float] = Field(None, description="Precipitation in centimeters")
    
    class Config:
        json_schema_extra = {
            "example": {
                "station_id": "USC00110072",
                "date": "19850101",
                "max_temp": 5.0,
                "min_temp": -1.1,
                "precipitation": 0.25
            }
        }


class YearlyStatsResponse(BaseModel):
    """Response model for yearly weather statistics"""
    station_id: str = Field(..., description="Weather station identifier")
    year: int = Field(..., description="Year")
    avg_max_temp: Optional[float] = Field(None, description="Average maximum temperature in degrees Celsius")
    avg_min_temp: Optional[float] = Field(None, description="Average minimum temperature in degrees Celsius")
    total_precipitation: Optional[float] = Field(None, description="Total accumulated precipitation in centimeters")
    
    class Config:
        json_schema_extra = {
            "example": {
                "station_id": "USC00110072", 
                "year": 1985,
                "avg_max_temp": 15.2,
                "avg_min_temp": 3.8,
                "total_precipitation": 45.67
            }
        }


class PaginationInfo(BaseModel):
    """Pagination information"""
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Number of items per page")
    total_items: int = Field(..., description="Total number of items")
    total_pages: int = Field(..., description="Total number of pages")
    has_next: bool = Field(..., description="Whether there is a next page")
    has_previous: bool = Field(..., description="Whether there is a previous page")


class WeatherDataListResponse(BaseModel):
    """Paginated response for weather data"""
    data: List[WeatherDataResponse]
    pagination: PaginationInfo


class YearlyStatsListResponse(BaseModel):
    """Paginated response for yearly statistics"""
    data: List[YearlyStatsResponse]
    pagination: PaginationInfo
