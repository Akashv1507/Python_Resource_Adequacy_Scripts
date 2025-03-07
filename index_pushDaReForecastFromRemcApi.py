from src.appConfig import loadAppConfig
import datetime as dt
import argparse
import logging
import pandas as pd
from src.reForecastDataInsertion import ReForecastInsertion
import requests
from cryptography.hazmat.primitives.kdf import pbkdf2

# Create and configure logger
logging.basicConfig(filename="files/logs/pushReForecast.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


appConfig = loadAppConfig()
dbConStr = appConfig['con_string_mis_warehouse']

endDate = dt.datetime.now()+dt.timedelta(days=1)
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
startDateStr = dt.datetime.strftime(startDate, '%Y-%m-%d')
endDateStr = dt.datetime.strftime(endDate, '%Y-%m-%d')
logger.info(f"------Insertion started of DayAhead solar&wind forecast b/w {startDateStr} To {endDateStr}--------- ")

obj_daReForecastInsertion=ReForecastInsertion(dbConStr)

stateList = appConfig['remcApiReStateObj']
remcApiBaseUrl = appConfig['remcApiBaseUrl']
remcDayAheadApiEndpoint =f"{remcApiBaseUrl}/api/dayAhead"
allStateReForecastDf= pd.DataFrame()

for state in stateList:
    entityId = state['entityId']
    stateCode = state['stateCode']
    currStateReForecastDf = pd.DataFrame()
    stateSolarForecastDf = pd.DataFrame()
    stateWindForecastDf = pd.DataFrame()
    # fetching SOLAR data for current state through REMC API, and concating in allStateReForecastDf
    try:
        response = requests.get(remcDayAheadApiEndpoint, params={"stateCode": stateCode, "genType": "SOLAR", "startDate": startDateStr, "endDate": endDateStr})
        if response.ok:
            responseData= response.json()
            stateSolarForecastDf = pd.DataFrame.from_dict(responseData['responseData']) 
            logger.info(f"Api Fetch was success for SOLAR/{stateCode}/->{responseData['responseText']}")
        else:
            responseData= response.json()
            logger.info(f"Api Fetch was aborted for SOLAR/{stateCode}->{responseData['responseText']}")
    except Exception as err:
        logger.exception(f"API call failed with error -> {err}")
        continue
    
    if not stateSolarForecastDf.empty:
        stateSolarForecastDf.columns= ['Time_Stamp', 'RE_Forecasted_Value']
        stateSolarForecastDf['Time_Stamp']=pd.to_datetime(stateSolarForecastDf['Time_Stamp'], format='%Y-%m-%d %H:%M')
        stateSolarForecastDf['Entity_Tag']=entityId
        stateSolarForecastDf['RE_Type']='SOLAR'
        allStateReForecastDf = pd.concat([allStateReForecastDf, stateSolarForecastDf])

    # fetching WIND data for current state through REMC API, and concating in allStateReForecastDf
    response = requests.get(remcDayAheadApiEndpoint, params={"stateCode": stateCode, "genType": "WIND", "startDate": startDateStr, "endDate": endDateStr})
    if response.ok:
        responseData= response.json()
        stateWindForecastDf = pd.DataFrame.from_dict(responseData['responseData'])
        logger.info(f"Api Fetch was success for WIND/{stateCode}/->{responseData['responseText']}") 
    else:
        responseData= response.json()
        logger.info(f"Api Fetch was aborted for SOLAR/{stateCode}->{responseData['responseText']}")

    if not stateWindForecastDf.empty:
        stateWindForecastDf.columns= ['Time_Stamp', 'RE_Forecasted_Value']
        stateWindForecastDf['Time_Stamp']=pd.to_datetime(stateWindForecastDf['Time_Stamp'], format='%Y-%m-%d %H:%M')
        stateWindForecastDf['Entity_Tag']=entityId
        stateWindForecastDf['RE_Type']='WIND'
        allStateReForecastDf = pd.concat([allStateReForecastDf, stateWindForecastDf])
            
if allStateReForecastDf.empty ==False:
    allStateReForecastDf.reset_index(inplace=True, drop=True)
    respObj = obj_daReForecastInsertion.pushDataToDb(allStateReForecastDf)
    if respObj['isInsertionSuccess']: 
        logger.info(f" Processing done for Dates {startDateStr} to {endDateStr}.... ")
    else:
        logger.error(f"No REForecast Data inserted in db. Check db logs/query")
else:
    logger.error(f"Neither wind/solar REForecast Fetched From REMC API for Dates {startDateStr} to {endDateStr}")


