from src.appConfig import loadAppConfig
import datetime as dt
import argparse
import logging
import pandas as pd
import os
from src.reForecastDataInsertion import ReForecastInsertion
from cryptography.hazmat.primitives.kdf import pbkdf2


# Create and configure logger
logging.basicConfig(filename="files/logs/pushReForecast.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


appConfig = loadAppConfig()
reForecastFolderPath = appConfig['reForecastFolderPath']
dbConStr = appConfig['con_string_mis_warehouse']

endDate = dt.datetime.now()
startDate = endDate 
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

logger.info(f"------Insertion started of Intraday solar&wind forecast file b/w {dt.datetime.strftime(startDate, '%Y-%m-%d')}{dt.datetime.strftime(endDate, '%Y-%m-%d')}--------- ")
obj_intradayReForecastInsertion=ReForecastInsertion(dbConStr)

stateList = appConfig['reStateObj']
reForecastFiles = os.listdir(reForecastFolderPath)
currdate = startDate
removeFilesPathList =[]

while currdate<=endDate:
    currdateStrYYYYMMDD= dt.datetime.strftime(currdate, '%Y-%m-%d')
    allStateReForecastDf= pd.DataFrame()
    for state in stateList:
        stateName = state['stateName']
        entityId = state['entityId']
        currStateReForecastDf = pd.DataFrame()
        stateSolarForecastDf = pd.DataFrame()
        stateWindForecastDf = pd.DataFrame()
        isFoundCurrDayCurStateSolarFile=False
        isFoundCurrDayCurStateWindFile=False
        # update 1 reading schedule column in case of gujarat ,becoz gujarat wind/solar forecast is incorrect B=Time, C-Forecast, F-Schedule
        # update 2 now gujarat wind/sola forecast coming correct. updating to original.
        usecolsVar= [1,2]
        # if stateName=='Gujarat':
        #     usecolsVar= [1,5]
        for singleReFile in reForecastFiles:
                    
            if (isFoundCurrDayCurStateSolarFile==False) and (currdateStrYYYYMMDD in singleReFile) and (stateName in singleReFile) and ('intraday' in singleReFile) and ('Solar' in singleReFile) and (singleReFile.endswith(('.csv'))):               
                solarForecastFileNamePath = reForecastFolderPath + singleReFile   
                stateSolarForecastDf = pd.read_csv(solarForecastFileNamePath, usecols= usecolsVar,skipfooter=1,engine='python')
                stateSolarForecastDf.columns= ['Time_Stamp', 'RE_Forecasted_Value']
                stateSolarForecastDf['Time_Stamp']=pd.to_datetime(stateSolarForecastDf['Time_Stamp'], format='%d-%m-%Y %H:%M')
                stateSolarForecastDf['Entity_Tag']=entityId
                stateSolarForecastDf['RE_Type']='SOLAR'
                allStateReForecastDf = pd.concat([allStateReForecastDf, stateSolarForecastDf])
                # if  solar file found  then setting isFound flag to true and adding current file path to removeable files list
                isFoundCurrDayCurStateSolarFile=True
                removeFilesPathList.append(solarForecastFileNamePath)

            if (isFoundCurrDayCurStateWindFile==False) and (currdateStrYYYYMMDD in singleReFile) and (stateName in singleReFile) and ('da' in singleReFile) and ('Wind' in singleReFile) and (singleReFile.endswith(('.csv'))):   
                windForecastFileNamePath = reForecastFolderPath + singleReFile
                stateWindForecastDf = pd.read_csv(windForecastFileNamePath, usecols= usecolsVar, skipfooter=1, engine='python')
                stateWindForecastDf.columns= ['Time_Stamp', 'RE_Forecasted_Value']
                stateWindForecastDf['Time_Stamp']=pd.to_datetime(stateWindForecastDf['Time_Stamp'], format='%d-%m-%Y %H:%M')
                stateWindForecastDf['Entity_Tag']=entityId
                stateWindForecastDf['RE_Type']='WIND'
                allStateReForecastDf = pd.concat([allStateReForecastDf, stateWindForecastDf])
                # if wind file found  then setting isFound flag to true and adding current file path to removeable files list
                isFoundCurrDayCurStateWindFile=True
                removeFilesPathList.append(windForecastFileNamePath)
            # if both solar and wind file found for curr date and curr state, then exiting loop of all files    
            if isFoundCurrDayCurStateSolarFile == True and isFoundCurrDayCurStateWindFile ==True:
                logger.info(f"Found both Solar and wind file {stateName}-{currdateStrYYYYMMDD} and appended to dataframe")
                break
        # if  solar or wind file not found for curr date and curr state after iterating through all files, then logging warning
        if isFoundCurrDayCurStateSolarFile == False:
            logger.warning(f"Please put {stateName}-Solar file of {currdateStrYYYYMMDD} and Re-execute")
        if isFoundCurrDayCurStateWindFile == False:
            logger.warning(f"Please put {stateName}-Wind file of {currdateStrYYYYMMDD} and Re-execute")

    if allStateReForecastDf.empty ==False:
        allStateReForecastDf.reset_index(inplace=True, drop=True)
        respObj = obj_intradayReForecastInsertion.pushDataToDb(allStateReForecastDf)
        if respObj['isInsertionSuccess']:
            # now removing all files that are processed
            for removableFile in removeFilesPathList:
                os.remove(removableFile)
        logger.info(f" Processing done for Date {currdateStrYYYYMMDD}")
    else:
        logger.info(f" No State Wind/Solar file found for Date {currdateStrYYYYMMDD}")
    currdate = currdate + dt.timedelta(days=1)


