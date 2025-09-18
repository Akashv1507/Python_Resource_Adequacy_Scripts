import oracledb as cx_Oracle
import pandas as pd
from typing import List, Tuple
import logging



# Create and configure logger
logging.basicConfig(filename="files/logs/pushForecast.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
# Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

class DemandForecastInsertion():
    """
        Forecast Insertion To DB
    """

    def __init__(self, con_string):
        """initialize connection string
        Args:
            con_string ([type]): connection string 
        """
        self.connString = con_string
      

    def pushDataToDb(self, forecastDf:pd.core.frame.DataFrame) -> str:
        """
        Args:
            forecastDf(dataframe): forecast excel file data frame
           
        Returns:
            msg(str): resp msg if push successfull or not
        """  
        forecastDf= forecastDf.rename(columns=str.upper)
        
        forecastDf.rename(columns = {'WR':'WRLDCMP.SCADA1.A0047000', 'MAHARASHTRA': 'WRLDCMP.SCADA1.A0046980', 'GUJARAT':'WRLDCMP.SCADA1.A0046957', 
                                     'MP': 'WRLDCMP.SCADA1.A0046978', 'CHHATTISGARH':'WRLDCMP.SCADA1.A0046945', 'GOA':'WRLDCMP.SCADA1.A0046962', 'DNHDDPDCL': 'WRLDCMP.SCADA3.A0134103'}, inplace = True)
        
        # rounding to nearest minutes to avoid exception
        #forecastDf['TIME'] =forecastDf['TIME'].dt.round('min')

        #selecting table name on basis of model Name
        forecastTableName = "dayahead_demand_forecast"
        revisionTableName ="forecast_revision_store"
        columnsName =  forecastDf.columns.tolist()
        stateColumnName = columnsName[1:]

        demandList:List[Tuple] = []
        demandListR0A:List[Tuple] = []
        for ind in forecastDf.index:
            for entity in stateColumnName:
                tempTuple = (forecastDf['TIME BLOCK'][ind], entity, int(forecastDf[entity][ind]))
                demandList.append(tempTuple)

                tempTupleR0A = (forecastDf['TIME BLOCK'][ind], entity, 'R0A', int(forecastDf[entity][ind]))
                demandListR0A.append(tempTupleR0A)

        # making list of tuple of timestamp(unique),entityTag based on which deletion takes place before insertion of duplicate
        existingRows = [(x[0],x[1]) for x in demandList]
        existingRowsR0A = [(x[0],x[1], x[2]) for x in demandListR0A]

        try:  
            connection = cx_Oracle.connect(self.connString)
            isInsertionSuccess = True
        except Exception as err:
            logger.error(f'error while creating a connection {err}')
            isInsertionSuccess=False
        else:
            try:
                cur = connection.cursor()
                cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
                del_sql = f"DELETE FROM {forecastTableName} WHERE time_stamp = :1 and entity_tag=:2"
                cur.executemany(del_sql, existingRows)
                insert_sql = f"INSERT INTO {forecastTableName}(time_stamp,ENTITY_TAG,forecasted_demand_value) VALUES(:1, :2, :3)"
                cur.executemany(insert_sql, demandList)

                # stroing R0A
                del_sql = f"DELETE FROM {revisionTableName} WHERE time_stamp = :1 and entity_tag=:2 and revision_no =:3"
                cur.executemany(del_sql, existingRowsR0A)
                insert_sql = f"INSERT INTO {revisionTableName}(time_stamp,ENTITY_TAG,revision_no,forecasted_demand_value) VALUES(:1, :2, :3, :4)"
                cur.executemany(insert_sql, demandListR0A)
            
            except Exception as err:
                logger.error(f'error while creating a cursor/executing sql {err}')
                isInsertionSuccess = False
            else:
                connection.commit()
            finally:
                cur.close()
        finally:  
            connection.close()
        if isInsertionSuccess:
            msg = "Forecast Excel Upload Successfull !!!!"
        else:
            msg = "Forecast Excel Upload UnSuccessfull!!! Please Try Again"
        return msg