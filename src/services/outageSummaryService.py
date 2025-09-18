import datetime as dt
import requests
import json
import oracledb as cx_Oracle
import logging
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from src.appConfig import getAppConfig
import pandas as pd
from src.sqls.genMetaDataFetchSql import genMetaDataFetchSql
from src.sqls.outagefetchForDatetimeSql import outageFetchSql


# Create and configure logger
logging.basicConfig(filename="files/logs/pushOutageSummary.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
# Creating an object
loggerr = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
loggerr.setLevel(logging.DEBUG)

class OutageSummaryService:
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

    def fetchGenMetaData(self):
        generatingUnitsMetaDataDf = pd.DataFrame()
        if not self.outageDbConnection:
            loggerr.info("No active outage database connection.")
            return generatingUnitsMetaDataDf
        try:  
            generatingUnitsMetaDataDf = pd.read_sql(genMetaDataFetchSql, con=self.outageDbConnection)
        except Exception as e:
            loggerr.exception(f"Error during data fetch: {str(e)}")
        finally:
           return generatingUnitsMetaDataDf
    
    def fetchOutageData(self, timestamp:dt.datetime):
        outageDataDf= pd.DataFrame()
        timestampStr = dt.datetime.strftime(timestamp, '%Y-%m-%d %H:%M:%S')
        if not self.outageDbConnection:
            loggerr.info("No active outage database connection.")
            return outageDataDf
        try:  
            outageDataDf = pd.read_sql(outageFetchSql, params={'targetDatetime':timestampStr }, con=self.outageDbConnection)
        except Exception as e:
            loggerr.exception(f"Error during data fetch: {str(e)}")
        finally:
            return outageDataDf
    
    def fetchStateDcData(self, revisionTypeTableName:str, startTime:dt.datetime, endTime:dt.datetime):
        """Fetch all state dc between starttime and endtime"""

        stateDcDataDf = pd.DataFrame()
        if not self.raDbConnection:
            print("No active postgresql RA database connection.")
            return stateDcDataDf
        # handling sql injection
        allowed_tables = {"day_ahead_dc_data", "intraday_dc_data"}
        if revisionTypeTableName not in allowed_tables:
            return stateDcDataDf
        # Compose SQL with safe identifiers for table names
        query = f"""select dadd.date_time , mt.state , mt.fuel_type,  sum(dadd.dc_data) dc_data
                            FROM {revisionTypeTableName} dadd JOIN mapping_table mt ON dadd.plant_id = mt.id
                            where dadd.date_time between %(start_time)s AND %(end_time)s
                            group by dadd.date_time, mt.state, mt.fuel_type order by 
                            mt.state, dadd.date_time, mt.fuel_type;
        """
        cursor = self.raDbConnection.cursor()
        try:
            cursor.execute(query, {"start_time": startTime,"end_time": endTime})            
            rows = cursor.fetchall()
            stateDcDataDf = pd.DataFrame(rows)
            # stateDcDataDf = pd.read_sql(query, params={"start_time": startTime,"end_time": endTime}, con=self.raDbConnection)
        except Exception as e:
            print(f"Error fetching State Dc data: {str(e)}")      
        finally:
            if cursor:
                cursor.close()
            return stateDcDataDf
        
    def insertOutageSummaryRows(self, outageSummaryAllRows:list[tuple]):
        if not self.raDbConnection:
            print("No active RA database connection.")
            return False
        isInsertionSuccessfull= False
        cursor = self.raDbConnection.cursor()
        insertQuery = """
        INSERT INTO state_dc_outage_summary
        (timestamp, state_name, fuel_type, outage_capacity, normative_dc, intraday_dc, dayahead_dc)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (timestamp, state_name, fuel_type)
        DO UPDATE SET
            outage_capacity = EXCLUDED.outage_capacity,
            normative_dc = EXCLUDED.normative_dc,
            intraday_dc = EXCLUDED.intraday_dc,
            dayahead_dc = EXCLUDED.dayahead_dc;
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