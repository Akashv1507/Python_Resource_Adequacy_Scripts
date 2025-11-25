import datetime as dt
import oracledb as cx_Oracle
import logging
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from src.appConfig import getAppConfig
import pandas as pd
from src.sqls.genAllTypeOutagefetchForDatetimeSql import genAllTypeOutageFetchSql
from src.sqls.outagefetchForDatetimeSql import outageFetchSql


# Create and configure logger
logging.basicConfig(filename="files/logs/pushGenAllTypeOutageSummary.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
# Creating an object
loggerr = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
loggerr.setLevel(logging.DEBUG)

class GenAllTypeOutageSummaryService:
    def __init__(self, postgresqlHost:str, postgresqlPort:int, postgresqlDbName:str, postgresqlUser:str, postgresqlPwd:str,conStringOutageDb:str, instantClientPath:str):
        self.postgresqlHost= postgresqlHost
        self.postgresqlPort= postgresqlPort
        self.postgresqlDbName= postgresqlDbName
        self.postgresqlUser= postgresqlUser
        self.postgresqlPwd= postgresqlPwd
        self.conStringOutageDb = conStringOutageDb
        self.instantClientPath = instantClientPath
        self.raDbConnection = None
        self.outageDbConnection = None
  
    def connectOutgaeDb(self):
        """Establish a outage database connection."""
        if self.outageDbConnection:
            self.disconnectOutgaeDb()
        try:
            cx_Oracle.init_oracle_client(lib_dir=self.instantClientPath)
            self.outageDbConnection = cx_Oracle.connect(self.conStringOutageDb)
            loggerr.info("Connected to Oracle outage DB")
        except Exception as e:
            loggerr.error(f"Oracle DB connection error: {str(e)}")
            self.outageDbConnection = None

    def disconnectOutgaeDb(self):
        """Close the outage database connection."""
        if self.outageDbConnection:
            loggerr.info("Closing Oracle (Outage) DB connection")
            self.outageDbConnection.close()
            self.outageDbConnection = None

    def connectRaPostgresDb(self):
        """Establish a postgresql database connection."""
        if self.raDbConnection:
            self.disconnectRaPostgresDb()
        try:
            self.raDbConnection = psycopg.connect(dbname=self.postgresqlDbName,
                            user=self.postgresqlUser,
                            password=self.postgresqlPwd,
                            host=self.postgresqlHost,
                            port=self.postgresqlPort, row_factory=dict_row)
            loggerr.info("Connected to PostgreSql RA DB")
        except Exception as e:
            loggerr.error(f"RA postgresql Database connection error: {str(e)}")
            self.raDbConnection = None
    
    def disconnectRaPostgresDb(self):
        """Close the postgresql database connection."""
        if self.raDbConnection:
            loggerr.info("Closing Postgres (RA) DB connection")
            try:
                self.raDbConnection.commit()
            except Exception as e:
                loggerr.warning(f"Postgres commit failed before closing: {str(e)}")
                self.raDbConnection.rollback()
            finally:
                self.raDbConnection.close()
                self.raDbConnection = None
    
    def fetchGenAllTypeOutageData(self, timestamp:dt.datetime):
        outageDataDf= pd.DataFrame()
        timestampStr = dt.datetime.strftime(timestamp, '%Y-%m-%d %H:%M:%S')
        if not self.outageDbConnection:
            loggerr.info("No active outage database connection.")
            return outageDataDf
        try:  
            outageDataDf = pd.read_sql(genAllTypeOutageFetchSql, params={'targetDatetime':timestampStr }, con=self.outageDbConnection)
        except Exception as e:
            loggerr.exception(f"Error during data fetch: {str(e)}")
        finally:
            return outageDataDf
        
    def insertGenAllTypeOutageSummaryRows(self, outageSummaryAllRows:list[tuple]):
        if not self.raDbConnection:
            print("No active RA database connection.")
            return False
        isInsertionSuccessfull= False
        cursor = self.raDbConnection.cursor()
        insertQuery = """
                            INSERT INTO gen_all_outage_Type_summary (time_stamp, state_name, classification, station_type, shutdown_type, shutdown_tag, outage_val)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (time_stamp, state_name, classification, station_type, shutdown_type, shutdown_tag)
                            DO UPDATE SET
                                outage_val = EXCLUDED.outage_val;
                            """
        try:
            cursor.executemany(insertQuery, outageSummaryAllRows)
            self.raDbConnection.commit()
            isInsertionSuccessfull= True
        except Exception as e:
            print(f"Error pushing outage summary data: {str(e)}")
        finally:
            if cursor:
                cursor.close()
            return isInsertionSuccessfull