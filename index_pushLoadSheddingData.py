from src.appConfig import loadAppConfig
from src.services.loadSheddingDataService import LoadSheddingDataService
import datetime as dt
import logging
import pandas as pd
import argparse
import os

# Create and configure logger
logging.basicConfig(filename="files/logs/pushLoadShedding.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


appConfig = loadAppConfig()
shortageLoadSheddingFolderPath= appConfig['shortageLoadSheddingFolderPath']
stateCodeMapLs= appConfig['stateCodeMapLs']

parser = argparse.ArgumentParser()
endDate = dt.datetime.now() - dt.timedelta(days=1)
startDate = endDate - dt.timedelta(days=1)
parser.add_argument('--start_date', help="Enter Start date in yyyy-mm-dd format",
                    default=dt.datetime.strftime(startDate, '%Y-%m-%d'))
parser.add_argument('--end_date', help="Enter end date in yyyy-mm-dd format",
                    default=dt.datetime.strftime(endDate, '%Y-%m-%d'))
                   
args = parser.parse_args()
startDate = dt.datetime.strptime(args.start_date, '%Y-%m-%d')
endDate = dt.datetime.strptime(args.end_date, '%Y-%m-%d')
startDate = startDate.replace(hour=0, minute=0, second=0, microsecond=0)
endDate = endDate.replace(hour=0, minute=0, second=0, microsecond=0)
logger.info(f"------Insertion started Load Shedding File between {startDate.date()}- {endDate.date()}------- ")
#creating LoadShedding Service
loadSheddingDataService = LoadSheddingDataService(appConfig["postgresqlHost"], appConfig["postgresqlPort"], appConfig["postgresqlDb"], appConfig["postgresqlUser"], appConfig["postgresqlPass"] )

#records holds all  date between startDate and endDate data
records= []
currentDate = startDate
while currentDate<= endDate:
    yearStr = currentDate.strftime('%Y')
    # Month string can be any like Janary, Jan etc
    monthStrCaps = currentDate.strftime('%B')
    monthStrSmall = currentDate.strftime('%b')
    fileDateStr = currentDate.strftime('%d.%m.%Y')
    fileName = f"WR_shortage details_{fileDateStr}.xlsx"

    dynamicPath1 = os.path.join(shortageLoadSheddingFolderPath, yearStr, monthStrCaps)
    fullFilePath1 = os.path.join(dynamicPath1, fileName)
    dynamicPath2= os.path.join(shortageLoadSheddingFolderPath, yearStr, monthStrSmall)
    fullFilePath2 = os.path.join(dynamicPath2, fileName)

    for fullFilePath in [fullFilePath1, fullFilePath2]:
        if os.path.exists(fullFilePath):
            #nrows after skipping rows , index_col after useCols
            df = pd.read_excel(fullFilePath, skiprows=1, nrows=7, usecols= [i for i in range(0,25)], index_col=0, sheet_name='WR', engine='openpyxl')
            timeBlocks=loadSheddingDataService.generate15minBlocks(currentDate)
            for state, row in df.iterrows():
                stateCode = stateCodeMapLs.get(state)
                ls_96 = loadSheddingDataService.expandHourlyTo15min(row.tolist())
                for ts, val in zip(timeBlocks, ls_96):
                    records.append((ts, stateCode, float(val)))
            break
        else:
            logger.info(f"File not found: {fullFilePath}")
    currentDate = currentDate + dt.timedelta(days=1)

loadSheddingDataService.connect()
loadSheddingDataService.insertStateLsRecords(records)
loadSheddingDataService.disconnect()



