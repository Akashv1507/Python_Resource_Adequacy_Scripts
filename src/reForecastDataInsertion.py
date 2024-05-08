import oracledb as cx_Oracle
import pandas as pd
from typing import List, Tuple
import logging



# Create and configure logger
logging.basicConfig(filename="files/logs/pushReForecast.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
# Creating an object
loggerr = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
loggerr.setLevel(logging.DEBUG)

class ReForecastInsertion():
    """
        RE Forecast Insertion To DB
    """

    def __init__(self, con_string):
        """initialize connection string
        Args:
            con_string ([type]): connection string 
        """
        self.connString = con_string
      

    def pushDataToDb(self, reForecastDf:pd.core.frame.DataFrame) -> str:
        """
        Args:
            reForecastDf(dataframe):  Re forecast excel file data frame
           
        Returns:
            msg(str): resp msg if push successfull or not
        """  
        

        #selecting table name on basis of model Name
        forecastTableName = "re_forecast"

        daReForecastList:List[Tuple] = []
        
        for ind in reForecastDf.index:
                tempTupleR0A = (reForecastDf['Time_Stamp'][ind], reForecastDf['Entity_Tag'][ind], reForecastDf['RE_Type'][ind], reForecastDf["RE_Forecasted_Value"][ind])
                daReForecastList.append(tempTupleR0A)

        # making list of tuple of timestamp(unique),entityTag, re_type based on which deletion takes place before insertion of duplicate
        existingRowsR0A = [(x[0],x[1], x[2]) for x in daReForecastList]
        connection = None
        cur=None
        try:  
            connection = cx_Oracle.connect(self.connString)
            isInsertionSuccess = True  
        except Exception as err:
            loggerr.exception(f'error while creating a connection {err}')
            isInsertionSuccess=False
        else:
            try:
                cur = connection.cursor()
                cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MM-YYYY HH24:MI:SS' ")
                # stroing DA RE forecast
                del_sql = f"DELETE FROM {forecastTableName} WHERE TIME_STAMP = :1 and ENTITY_TAG=:2 and RE_TYPE =:3"
                cur.executemany(del_sql, existingRowsR0A)
                insert_sql = f"INSERT INTO {forecastTableName}(TIME_STAMP,ENTITY_TAG,RE_TYPE,RE_FORECASTED_VALUE) VALUES(:1, :2, :3, :4)"
                cur.executemany(insert_sql, daReForecastList)
            
            except Exception as err:
                loggerr.exception('error while creating a cursor/executing sql', err)
                isInsertionSuccess = False
            else:
                connection.commit()
            finally:
                if cur:
                    cur.close()
        finally: 
            if connection: 
                connection.close()
        if isInsertionSuccess:
           respObj = {'msg': "RE Forecast Upload Successfull !!!!", 'isInsertionSuccess':isInsertionSuccess}
        else:
            respObj = {"msg": "RE Forecast Upload UnSuccessfull!!! Please Try Again", 'isInsertionSuccess':isInsertionSuccess}
        return respObj