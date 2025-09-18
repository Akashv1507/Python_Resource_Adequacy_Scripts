from src.appConfig import loadAppConfig
from src.generateDcSdlActDataForScada.dcSdlActDataForScadaGenerator import DcSdlActDataGeneratorForScada
import datetime as dt
import logging
import pandas as pd




# Create and configure logger
logging.basicConfig(filename="files/logs/GenerateDcSdlActDataForScada.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='w')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


appConfig = loadAppConfig()

dbConStr = appConfig['con_string_mis_warehouse']
postgresqlUser = appConfig["postgresqlUser"]
postgresqlPass = appConfig["postgresqlPass"]
postgresqlDb = appConfig["postgresqlDb"]
postgresqlHost= appConfig["postgresqlHost"]
postgresqlPort = appConfig["postgresqlPort"]
tokenUrl = appConfig['tokenUrl']
apiBaseUrl = appConfig['apiBaseUrl']
clientId = appConfig['clientId']
clientSecret = appConfig['clientSecret']

targetTime = dt.datetime.now()
currentBlkTime= targetTime
while currentBlkTime.minute%15!=0:
    currentBlkTime=currentBlkTime-dt.timedelta(minutes=1)

currentBlkTime = currentBlkTime.replace(second=0, microsecond=0)
blkNo = int(currentBlkTime.hour *4 + currentBlkTime.minute/15 + 1)

logger.info(f"processing for {currentBlkTime}/{blkNo}")

obj_dcSdlActDataGeneratorForScada=DcSdlActDataGeneratorForScada(postgresqlUser, postgresqlPass, postgresqlDb, postgresqlHost, postgresqlPort, tokenUrl, apiBaseUrl, clientId, clientSecret)
obj_dcSdlActDataGeneratorForScada.getDcSdlActData(currentBlkTime, blkNo, appConfig)
