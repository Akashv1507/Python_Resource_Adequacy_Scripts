from src.generateDcSdlActDataForScada.scadaApiFetcher import ScadaApiFetcher
import datetime as dt
import psycopg2
import pandas as pd 
import logging



# Create and configure logger
logging.basicConfig(filename="files/logs/GenerateDcSdlActDataForScada.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='w')
# Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

class DcSdlActDataGeneratorForScada():

    def __init__(self, userName:str, pwd:str, dbName:str, host:str, port:int, tokenUrl:str, apiBaseUrl:str, clientId:str, clientSecret:str):
        self.userName= userName
        self.pwd= pwd
        self.dbName= dbName
        self.host=host
        self.port=port
        self.tokenUrl=tokenUrl
        self.apiBaseUrl=apiBaseUrl
        self.clientId=clientId 
        self.clientSecret= clientSecret
        

    def getScadaData(self, scadaPoint:str, plantName:str, currentTime:dt.datetime):
        strippedScadaPoint= scadaPoint.strip()
        obj_scadaApiFetcher= ScadaApiFetcher(self.tokenUrl, self.apiBaseUrl, self.clientId, self.clientSecret)
        resData = obj_scadaApiFetcher.fetchData(strippedScadaPoint, currentTime, currentTime)
        if len(resData)!=0:
            return resData[len(resData)-1]
        else:
            logger.info(f'act data fetch was not successfull for {plantName} -> {scadaPoint}')
            return None
    
    def getStaticData (self):

        staticDataDf= pd.DataFrame()
        connection = None
        try:
            connection = psycopg2.connect(database = self.dbName, user = self.userName, host= self.host, password = self.pwd, port = self.port)
        except Exception as err:
            logger.error(f'error while creating a connection for static data and error is {err}')
        else:
            try:    
                fetch_sql = "SELECT * FROM mapping_table"
                staticDataDf = pd.read_sql(fetch_sql, con=connection)
            except Exception as err:
                logger.error(f'error while creating a cursor/executing query for static data and error is {err}')    
            else:
                connection.commit()    
        finally:
            if connection:
                connection.close()
        return staticDataDf


    def getDcOrSdlData(self, currentTime:dt.datetime, tblName:str):

        currentTimeStr = dt.datetime.strftime(currentTime, '%Y-%m-%d %H:%M:%S')
        dcDataDf= pd.DataFrame()
        connection = None
        try:
            connection = psycopg2.connect(database = self.dbName, user = self.userName, host= self.host, password = self.pwd, port = self.port)
        except Exception as err:
            logger.error(f'error while creating a connection and error is {err}')
        else:
            try:    
                fetch_sql = f"SELECT * FROM {tblName} WHERE date_time =  %(currentTime)s  ORDER BY plant_name"
                dcDataDf = pd.read_sql(fetch_sql, params={'currentTime': currentTimeStr, }, con=connection) 
            except Exception as err:
                logger.error(f'error while creating a cursor/executing query and error is {err}')    
            else:
                connection.commit()
        finally:
            if connection:
                connection.close()
        return dcDataDf
        

    def getDcSdlActData(self, currentTime:dt.datetime, blkNo:int, appConfig:dict)->None:
        
        allPlantDcSdlActData = []
        sdlActDcDumpFolderPath = appConfig['sdlActDcDumpFolderPath']
        staticDataDf = self.getStaticData()
        dcDataDf = self.getDcOrSdlData(currentTime, 'intraday_dc_data')
        sdlDataDf = self.getDcOrSdlData(currentTime, 'intraday_sch_data')
        groupDf = staticDataDf.groupby('plant_name')
        for plantName, plantGroupDf in groupDf:
            plantDcDataDf = dcDataDf[dcDataDf['plant_name']==plantName]
            if plantDcDataDf.empty ==False:
                plantDcVal = dcDataDf[dcDataDf['plant_name']==plantName]['dc_data'].sum(axis = 0, skipna = True)
                plantSdlVal=sdlDataDf[sdlDataDf['plant_name']==plantName]['sch_data'].sum(axis = 0, skipna = True)
                installedCap= plantGroupDf['installed_capacity'].sum(axis=0, skipna=True)
                plantActVal =0
                for ind in plantGroupDf.index:
                    scadaIdList = plantGroupDf['scada_id'][ind].split(',')
                    sumScadaActVal= 0
                    for scadaId in scadaIdList:
                        scadaActVal = self.getScadaData(scadaId, plantName, currentTime)
                        if scadaActVal !=None:
                            sumScadaActVal= sumScadaActVal + scadaActVal[1]
                    plantActVal= plantActVal + sumScadaActVal

                allPlantDcSdlActData.append({'Timestamp':currentTime, 'Blk No': blkNo, 'Gen Name':plantName, 'Installed Capacity': installedCap, 'DC Val': plantDcVal,'Sdl Val': plantSdlVal ,'Act Val':plantActVal})
        allPlantDcSdlActDataDf = pd.DataFrame.from_dict(allPlantDcSdlActData)
        sdlActDcDumpFilepath = f'{sdlActDcDumpFolderPath}Resource_Adequacy_DcSdlAct.csv'
        allPlantDcSdlActDataDf.to_csv(sdlActDcDumpFilepath, index=False)



