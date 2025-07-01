import datetime as dt
import psycopg
from psycopg.rows import dict_row
import logging

# Create and configure logger
logging.basicConfig(filename="files/logs/pushLoadShedding.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


class LoadSheddingDataService:
    def __init__(self, host:str, port:int, dbName:str, user:str, password:str):
        self.host= host
        self.port= port
        self.dbName= dbName
        self.user= user
        self.password= password
        self.connection = None

    def generate15minBlocks(self, baseDate:dt.datetime):
        """Generate 96 datetime blocks at 15-min interval for a given date."""
        # baseTime = dt.datetime.strptime(base_date, "%Y-%m-%d")
        return [baseDate + dt.timedelta(minutes=15 * i) for i in range(96)]
    
    def expandHourlyTo15min(self, hourlyVals:list):
        """Convert 24 hourly values into 96 block values (each repeated 4 times)."""
        return [val for val in hourlyVals for _ in range(4)]
    
    def connect(self):
        """Establish a postgresql database connection."""
        try:
            self.connection = psycopg.connect(dbname=self.dbName,
                            user=self.user,
                            password=self.password,
                            host=self.host,
                            port=self.port, row_factory=dict_row)
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            self.connection = None
    
    def disconnect(self):
        """Close the PostgreSQL database connection."""
        if self.connection:
            self.connection.close()
    
    def insertStateLsRecords(self, records:list ):
        """Insert state Load Shedding data present in records variable to postgresql db."""
        if not self.connection:
            logger.error("No active database connection.")
            return
        
        cursor = self.connection.cursor()
        insertQuery = """
        INSERT INTO state_load_shedding (timestamp, state_code, ls_val)
        VALUES (%s, %s, %s)
        ON CONFLICT (timestamp, state_code) DO UPDATE SET ls_val = EXCLUDED.ls_val;
        """
        try:
            cursor.executemany(insertQuery, records)
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error fetching mapping data: {str(e)}")
        finally:
            cursor.close()
    
    