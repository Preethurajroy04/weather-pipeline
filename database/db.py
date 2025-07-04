import sqlite3


class Database:
    def __init__(self, db_path='weather.db'):
        """Initialize database connection"""
        self.db_path = db_path
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Connect to the SQLite database"""
        try:
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.cursor = self.connection.cursor()
            print(f"Successfully connected to database at {self.db_path}")
            return True
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    def create_tables(self):
        """Create weather data and yearly statistics tables if they don't exist"""
        try:
            # Create main weather data table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS weather_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                date TEXT NOT NULL,
                max_temp INTEGER,
                min_temp INTEGER,
                precipitation INTEGER,
                UNIQUE(station_id, date)
            )
            ''')
            
            # Create yearly statistics table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS yearly_weather_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                year INTEGER NOT NULL,
                avg_max_temp REAL,
                avg_min_temp REAL,
                total_precipitation REAL,
                UNIQUE(station_id, year)
            )
            ''')
            
            self.connection.commit()
            print("Weather data and yearly statistics tables created successfully")
            return True
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")
            return False
    
    def insert_weather_data(self, station_id, date, max_temp, min_temp, precipitation):
        """Insert weather data into the table"""
        try:
            # Convert -9999 to None (NULL in database)
            max_temp = None if max_temp == -9999 else max_temp
            min_temp = None if min_temp == -9999 else min_temp
            precipitation = None if precipitation == -9999 else precipitation
            
            self.cursor.execute('''
            INSERT OR REPLACE INTO weather_data 
            (station_id, date, max_temp, min_temp, precipitation)
            VALUES (?, ?, ?, ?, ?)
            ''', (station_id, date, max_temp, min_temp, precipitation))
            self.connection.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error inserting data: {e}")
            return False
    
    def insert_many_weather_data(self, data_list):
        """Insert multiple weather data records at once"""
        try:
            # Convert -9999 to None (NULL in database) for each record
            converted_data = []
            for record in data_list:
                station_id, date, max_temp, min_temp, precipitation = record
                max_temp = None if max_temp == -9999 else max_temp
                min_temp = None if min_temp == -9999 else min_temp
                precipitation = None if precipitation == -9999 else precipitation
                converted_data.append((station_id, date, max_temp, min_temp, precipitation))
            
            self.cursor.executemany('''
            INSERT OR REPLACE INTO weather_data 
            (station_id, date, max_temp, min_temp, precipitation)
            VALUES (?, ?, ?, ?, ?)
            ''', converted_data)
            self.connection.commit()

            return True
        except sqlite3.Error as e:
            print(f"Error inserting bulk data: {e}")
            return False
    
    def insert_yearly_stats(self, station_id, year, avg_max_temp, avg_min_temp, total_precipitation):
        """Insert yearly weather statistics into the table"""
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO yearly_weather_stats 
            (station_id, year, avg_max_temp, avg_min_temp, total_precipitation)
            VALUES (?, ?, ?, ?, ?)
            ''', (station_id, year, avg_max_temp, avg_min_temp, total_precipitation))
            self.connection.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error inserting yearly stats: {e}")
            return False
    
    def insert_many_yearly_stats(self, stats_list):
        """Insert multiple yearly statistics records at once"""
        try:
            self.cursor.executemany('''
            INSERT OR REPLACE INTO yearly_weather_stats 
            (station_id, year, avg_max_temp, avg_min_temp, total_precipitation)
            VALUES (?, ?, ?, ?, ?)
            ''', stats_list)
            self.connection.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error inserting bulk yearly stats: {e}")
            return False
    
    def query_data(self, query, params=None):
        """Execute a custom query and return the results"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Error executing query: {e}")
            return None
    
    def calculate_yearly_stats(self):
        """Calculate yearly statistics from weather data and store in yearly_stats table"""
        try:
            # Query to calculate yearly statistics
            query = '''
            INSERT OR REPLACE INTO yearly_weather_stats 
            (station_id, year, avg_max_temp, avg_min_temp, total_precipitation)
            SELECT 
                station_id,
                CAST(substr(date, 1, 4) AS INTEGER) as year,
                CASE 
                    WHEN COUNT(CASE WHEN max_temp != -9999 THEN 1 END) = 0 THEN NULL
                    ELSE ROUND(AVG(CASE WHEN max_temp != -9999 THEN max_temp / 10.0 END), 2)
                END as avg_max_temp,
                CASE 
                    WHEN COUNT(CASE WHEN min_temp != -9999 THEN 1 END) = 0 THEN NULL
                    ELSE ROUND(AVG(CASE WHEN min_temp != -9999 THEN min_temp / 10.0 END), 2)
                END as avg_min_temp,
                CASE 
                    WHEN COUNT(CASE WHEN precipitation != -9999 THEN 1 END) = 0 THEN NULL
                    ELSE ROUND(SUM(CASE WHEN precipitation != -9999 THEN precipitation / 100.0 ELSE 0 END), 2)
                END as total_precipitation
            FROM weather_data
            GROUP BY station_id, CAST(substr(date, 1, 4) AS INTEGER)
            HAVING COUNT(*) > 0
            '''
            
            self.cursor.execute(query)
            self.connection.commit()
            
            # Get count of records inserted
            count = self.cursor.rowcount
            print(f"Successfully calculated and stored yearly statistics for {count} station-year combinations")
            return True
        except sqlite3.Error as e:
            print(f"Error calculating yearly statistics: {e}")
            return False
    
    def get_yearly_stats(self, station_id=None, year=None):
        """Retrieve yearly statistics with optional filtering"""
        try:
            query = '''
            SELECT station_id, year, avg_max_temp, avg_min_temp, total_precipitation 
            FROM yearly_weather_stats
            '''
            params = []
            
            conditions = []
            if station_id:
                conditions.append("station_id = ?")
                params.append(station_id)
            if year:
                conditions.append("year = ?")
                params.append(year)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY station_id, year"
            
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Error retrieving yearly statistics: {e}")
            return None


# Usage example
if __name__ == "__main__":
    db = Database()
    if db.connect():
        # Create both tables
        db.create_tables()
        
        # Example: Calculate yearly statistics from existing data
        db.calculate_yearly_stats()
        
        # Example: Query yearly statistics
        stats = db.get_yearly_stats()
        for stat in stats[:5]:  # Show first 5 records
            station_id, year, avg_max, avg_min, total_precip = stat
            print(f"Station: {station_id}, Year: {year}, Avg Max Temp: {avg_max}, Avg Min Temp: {avg_min}, Total Precipitation: {total_precip}")
        
        db.close()

