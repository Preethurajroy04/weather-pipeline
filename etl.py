import logging
import os
import glob
import time
from datetime import datetime
from typing import List, Tuple
import polars as pl
from database.db import Database

# Set up logging
logging.basicConfig(
    level=logging.INFO,
)

logger = logging.getLogger(__name__)
class WeatherDataETL:
    """ETL pipeline for processing weather data files"""
    
    def __init__(self, data_dir: str = "wx_data", batch_size: int = 10000, db_path: str = "weather.db"):
        """Initialize the ETL pipeline
        
        Args:
            data_dir: Directory containing weather data files
            batch_size: Number of records to process in each batch
            db_path: Path to the SQLite database
        """
        self.data_dir = data_dir
        self.batch_size = batch_size
        self.db_path = db_path
        self.db = Database(db_path)
        
        # Statistics
        self.total_records_processed = 0
        self.total_records_inserted = 0
        self.total_files_processed = 0
        self.failed_files = []
        
    def parse_weather_file(self, file_path: str) -> int:
        """Parse a weather data file in batches and process each batch immediately
        
        Args:
            file_path: Path to the weather data file
            
        Returns:
            Number of records successfully processed, or 0 if parsing fails
        """
        try:
            # Extract station ID from filename
            station_id = os.path.basename(file_path).replace('.txt', '')
            logger.info(f"Processing file: {file_path} in batches of {self.batch_size}")
            
            total_records_processed = 0
            batch_count = 0
            
            # Read file in batches using Polars
            batch_reader = pl.read_csv_batched(
                file_path,
                separator='\t',
                has_header=False,
                new_columns=['date', 'max_temp', 'min_temp', 'precipitation'],
                schema_overrides={
                    'date': pl.Utf8,
                    'max_temp': pl.Int32,
                    'min_temp': pl.Int32,
                    'precipitation': pl.Int32
                },
                batch_size=self.batch_size,
            )
            
            batches = batch_reader.next_batches(1)  # Get batches of specified size 
            # Process each batch using next_batches()
            while batches:
                try:
                    
                    for batch_df in batches:
                        batch_count += 1
                        
                        # Add station ID to the batch
                        batch_df = batch_df.with_columns(
                            pl.lit(station_id).alias('station_id')
                        )
                        
                        # Reorder columns
                        batch_df = batch_df.select(['station_id', 'date', 'max_temp', 'min_temp', 'precipitation'])
                        
                        # Validate the batch
                        validated_batch = self.validate_data(batch_df)
                        
                        # Convert to tuples and insert directly
                        if len(validated_batch) > 0:
                            batch_tuples = list(validated_batch.iter_rows())
                            records_inserted = self.process_batch(batch_tuples)
                            total_records_processed += records_inserted
                            
                            if batch_count % 10 == 0:  # Log every 10 batches
                                logger.info(f"Processed {batch_count} batches, {total_records_processed} records so far...")

                    # Get next batch
                    batches = batch_reader.next_batches(1)               
                except Exception as batch_error:
                    logger.error(f"Error processing batch {batch_count}: {str(batch_error)}")
                    break
            
            logger.info(f"Successfully processed {file_path}: {total_records_processed} records in {batch_count} batches")
            return total_records_processed
            
        except Exception as e:
            logger.error(f"Failed to process {file_path} in batches: {str(e)}")
            self.failed_files.append(file_path)
            return 0
    
    def validate_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate and clean weather data
        
        Args:
            df: Polars DataFrame with weather data
            
        Returns:
            Validated and cleaned DataFrame
        """
        original_count = len(df)
        
        # Replace missing values (-9999) with None
        df = df.with_columns([
            pl.when(pl.col('max_temp') == -9999).then(None).otherwise(pl.col('max_temp')).alias('max_temp'),
            pl.when(pl.col('min_temp') == -9999).then(None).otherwise(pl.col('min_temp')).alias('min_temp'),
            pl.when(pl.col('precipitation') == -9999).then(None).otherwise(pl.col('precipitation')).alias('precipitation')
        ])
        
        # Validate date format (should be YYYYMMDD)
        df = df.filter(
            pl.col('date').str.len_chars() == 8
        )
        
        # Remove duplicates based on station_id and date
        df = df.unique(subset=['station_id', 'date'])
        
        # Log validation results
        cleaned_count = len(df)
        if cleaned_count != original_count:
            logger.info(f"Validation: {original_count} -> {cleaned_count} records")
        
        return df
    
    def process_batch(self, batch_data: List[Tuple]) -> int:
        """Process a batch of data and insert into database
        
        Args:
            batch_data: List of tuples containing weather data
            
        Returns:
            Number of records successfully inserted
        """
        try:
            # Use the database's insert_many method which handles duplicates
            success = self.db.insert_many_weather_data(batch_data)
            
            if success:
                logger.debug(f"Successfully inserted batch of {len(batch_data)} records")
                return len(batch_data)
            else:
                logger.error(f"Failed to insert batch of {len(batch_data)} records")
                return 0
                
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            return 0
    
    
    def get_data_files(self) -> List[str]:
        """Get list of all weather data files
        
        Returns:
            List of file paths
        """
        pattern = os.path.join(self.data_dir, "*.txt")
        files = glob.glob(pattern)
        logger.info(f"Found {len(files)} weather data files")
        return files
    
    def run_etl(self):
        """Run the complete ETL process"""
        start_time = time.time()
        logger.info("=" * 50)
        logger.info("Starting Weather Data ETL Process")
        logger.info(f"Start time: {datetime.now()}")
        logger.info("=" * 50)
        
        try:
            # Initialize database
            if not self.db.connect():
                logger.error("Failed to connect to database")
                return
            
            self.db.create_tables()
            
            # Get all data files
            files = self.get_data_files()
            
            if not files:
                logger.warning("No data files found to process")
                return
            
            # Process each file
            for file_path in files:
                logger.info(f"Processing file: {os.path.basename(file_path)}")
                
                # Parse file in batches and process immediately
                records_inserted = self.parse_weather_file(file_path)
                
                if records_inserted > 0:
                    # Update statistics
                    self.total_records_inserted += records_inserted
                    self.total_files_processed += 1
                    
                    logger.info(f"File completed: {records_inserted} records inserted")
                else:
                    logger.warning(f"No records inserted for file: {os.path.basename(file_path)}")
            
            # Calculate yearly statistics after all files are processed
            if self.total_files_processed > 0:
                logger.info("=" * 50)
                logger.info("Calculating yearly statistics...")
                stats_start_time = time.time()
                
                stats_success = self.db.calculate_yearly_stats()
                
                stats_duration = time.time() - stats_start_time
                
                if stats_success:
                    logger.info(f"Yearly statistics calculated successfully in {stats_duration:.2f} seconds")
                    
                    # Get count of yearly stats records for reporting
                    stats_count = self.db.query_data("SELECT COUNT(*) FROM yearly_weather_stats")
                    if stats_count:
                        logger.info(f"Generated {stats_count[0][0]} yearly statistics records")
                else:
                    logger.error("Failed to calculate yearly statistics")
        
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            
        finally:
            # Close database connection
            self.db.close()
            
            # Log final statistics
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info("=" * 50)
            logger.info("ETL Process Complete")
            logger.info(f"End time: {datetime.now()}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Files processed: {self.total_files_processed}")
            logger.info(f"Total records inserted: {self.total_records_inserted}")
            
            if self.failed_files:
                logger.warning(f"Failed files: {len(self.failed_files)}")
                for failed_file in self.failed_files:
                    logger.warning(f"  - {failed_file}")
            
            logger.info("=" * 50)


def main():
    """Main function to run the ETL process"""
    
    
    # Configuration
    DATA_DIR = "wx_data"
    BATCH_SIZE = 10000
    DB_PATH = "weather.db"
    
    # Create and run ETL pipeline
    etl = WeatherDataETL(
        data_dir=DATA_DIR,
        batch_size=BATCH_SIZE,
        db_path=DB_PATH
    )
    
    etl.run_etl()


if __name__ == "__main__":
    main()