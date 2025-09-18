from src.appConfig import loadAppConfig
import datetime as dt
import argparse
import logging
import pandas as pd
from src.DemandForecastDataInsertion import DemandForecastInsertion
import sys
from cryptography.hazmat.primitives.kdf import pbkdf2


# Create and configure logger
logging.basicConfig(filename="files/logs/pushForecast.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


appConfig = loadAppConfig()
demForecastFolderPath = appConfig['demandForecastFolderPath']
dbConStr = appConfig['con_string_mis_warehouse']

endDate = dt.datetime.now() + dt.timedelta(days=1)
startDate = endDate 
# startDate = dt.datetime.strptime("2024-05-21", '%Y-%m-%d')
# endDate = dt.datetime.strptime("2024-05-21" , '%Y-%m-%d')
# get start and end dates from command line
parser = argparse.ArgumentParser()
parser.add_argument('--start_date', help="Enter Start date in yyyy-mm-dd format",
                    default=dt.datetime.strftime(startDate, '%Y-%m-%d'))
parser.add_argument('--end_date', help="Enter end date in yyyy-mm-dd format",
                    default=dt.datetime.strftime(endDate, '%Y-%m-%d'))
args = parser.parse_args()
startDate = dt.datetime.strptime(args.start_date, '%Y-%m-%d')
endDate = dt.datetime.strptime(args.end_date, '%Y-%m-%d')
startDate = startDate.replace(hour=0, minute=0, second=0, microsecond=0)
endDate = endDate.replace(hour=0, minute=0, second=0, microsecond=0)

logger.info(f"-----------startDate = {dt.datetime.strftime(startDate, '%Y-%m-%d')}, endDate = {dt.datetime.strftime(endDate, '%Y-%m-%d')}--------------")
obj_forecastInsertion=DemandForecastInsertion(dbConStr)

currdate = startDate

while currdate<=endDate:

    
    currdateStrDDMMYYYY= dt.datetime.strftime(currdate, '%d-%m-%Y')
    currdateStrYYYYMMDD= dt.datetime.strftime(currdate, '%Y-%m-%d')
    forecastFileName= f"State Forecast in MU-{currdateStrDDMMYYYY}.xlsx"
    forecastFileNamePath = demForecastFolderPath + forecastFileName
    forecastDf=None
    try:
        forecastDf = pd.read_excel(forecastFileNamePath, sheet_name=1, skiprows=1, usecols=range(1,9))
        forecastDf['Time Block'] = f'{currdateStrYYYYMMDD} ' +forecastDf['Time Block']
        logger.info(f"forecast file reading successfull for {currdateStrDDMMYYYY}")
    except Exception as err:
        logger.error(f"Error occured while reading excel for {currdateStrDDMMYYYY} and err is {err}")
        sys.exit()
    insertionMsg =obj_forecastInsertion.pushDataToDb(forecastDf)
    logger.info(f" {insertionMsg} for Date {currdateStrDDMMYYYY}")
    currdate = currdate + dt.timedelta(days=1)


