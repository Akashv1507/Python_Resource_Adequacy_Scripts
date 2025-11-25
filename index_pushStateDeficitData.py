from src.appConfig import loadAppConfig
from src.services.deficitCalculationService import DeficitCalculationService
import datetime as dt
import logging
import pandas as pd
import argparse
from cryptography.hazmat.primitives.kdf import pbkdf2

# Create and configure logger
logging.basicConfig(filename="files/logs/pushDeficitData.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)
appConfig = loadAppConfig()
deficitStateMetaData = appConfig['deficitStateData']

parser = argparse.ArgumentParser()
targetTime = dt.datetime.now()-dt.timedelta(days=1)
# targetTime = dt.datetime.now()
parser.add_argument('--target_time', help="Enter Target_time in yyyy-mm-dd HH:MM  format",
                    default=dt.datetime.strftime(targetTime, '%Y-%m-%d %H:%M'))               
args = parser.parse_args()
targetTime = dt.datetime.strptime(args.target_time, '%Y-%m-%d %H:%M')
#target time will calculate deficit intraday and daTargetTime will calculated deficit DA
# suppose you run this task at day D and time T, it will create intraday deficit revision for Day D, Dayahead deficit for day D+1 for same time T
targetTime = targetTime.replace(second=0, microsecond=0)
daTargetTime = targetTime + dt.timedelta(days=1)
startTargetTime = targetTime.replace(hour=0,minute=0, second=0, microsecond=0)
endTargetTime = targetTime.replace(hour=23,minute=59, second=0, microsecond=0)
startDaTargetTime = startTargetTime + dt.timedelta(days=1)
endDaTargetTime = endTargetTime + dt.timedelta(days=1)
allStateScadaId = []
allStateWbesAcr = []
allStateRaDbAcr = []
allStateReAcr = []
for stateMetaData in deficitStateMetaData:
    allStateScadaId.append(stateMetaData['stateScadaId'])
    allStateWbesAcr.append(stateMetaData['stateWbesAcr'])
    allStateRaDbAcr.append(stateMetaData['stateRaDbAcr'])
    allStateReAcr.append(stateMetaData['stateReAcr'])

logger.info(f"------Deficit calculation started for time {targetTime.date()}/{targetTime.time()}-----------------------")

deficitCalculationService = DeficitCalculationService(appConfig["postgresqlHost"], appConfig["postgresqlPort"], appConfig["postgresqlDb"], appConfig["postgresqlUser"], appConfig["postgresqlPass"], 
                                                      appConfig["conStringMisWarehouse"], appConfig["wbesApiUrl"], appConfig["wbesRevNoUrl"], appConfig["WbesApiUser"], appConfig["WbesApiPass"],
                                                        appConfig["wbesApikey"])
#all service method will return dictionary of dayahead and intraday revision no for current time
demForRevNoDict = deficitCalculationService.getDemandForecastRevNo(targetTime=targetTime)
reForeRevNoDict = deficitCalculationService.getReForecastRevNo(targetTime=targetTime)
scheduleRevNoDict = deficitCalculationService.getScheduleRevNO(targetTime=targetTime)
logger.info(demForRevNoDict, reForeRevNoDict, scheduleRevNoDict)
# fetching demand forecast data for intraday and dayahead deficit calculation
deficitCalculationService.connectMisWarehouseDb()
intradayDemForecast = deficitCalculationService.fetchDemandForecast(startTargetTime, endTargetTime, allStateScadaId, demForRevNoDict['intRevNo'])
dayaheadDemForecast = deficitCalculationService.fetchDemandForecast(startDaTargetTime, endDaTargetTime, allStateScadaId, demForRevNoDict['daRevNo'])
deficitCalculationService.disconnectMisWarehouseDb()

#fetching state dc data for intraday and dayahead deficit calculation
deficitCalculationService.connectRaPostgresDb()
daDcData = deficitCalculationService.fetchStateDcData("day_ahead_dc_data", startDaTargetTime, endDaTargetTime)
intradayDcData = deficitCalculationService.fetchStateDcData("intraday_dc_data", startTargetTime, endTargetTime)
deficitCalculationService.disconnectRaPostgresDb()

# fetching solar/wind RE forecast data
intradaySolarForecastData = deficitCalculationService.fetchReForecastData(startTargetTime, endTargetTime, "SOLAR", allStateReAcr, reForeRevNoDict['intRevNo'] )
intradayWindForecastData = deficitCalculationService.fetchReForecastData(startTargetTime, endTargetTime, "WIND", allStateReAcr, reForeRevNoDict['intRevNo'] )
# suppose running this script on current date(26) Morning for targetTime of 25(d-1), For this daTime will be of 26 hence dayahead revision will not be stored.
# hence first fetching from revision store api, if it is empty dayahead real time api will be called. this will be usefull for creating deficit data for D-4,D-5 day etc
dayaheadSolarForecastData = deficitCalculationService.fetchReForecastData(startDaTargetTime, endDaTargetTime,"SOLAR", allStateReAcr, reForeRevNoDict['daRevNo'] )
dayaheadWindForecastData = deficitCalculationService.fetchReForecastData(startDaTargetTime, endDaTargetTime,"WIND", allStateReAcr, reForeRevNoDict['daRevNo'] )
if len(dayaheadSolarForecastData)==0:
    dayaheadSolarForecastData = deficitCalculationService.fetchDaReForecastDataFromRealTimeApi(startDaTargetTime, endDaTargetTime, "SOLAR", allStateReAcr)
if len(dayaheadWindForecastData)==0:
    dayaheadWindForecastData = deficitCalculationService.fetchDaReForecastDataFromRealTimeApi(startDaTargetTime, endDaTargetTime, "WIND", allStateReAcr)

# handling special case for chattisgarh(Solar Forecast) as it is not coming from remc api . it is stored in local mis db
deficitCalculationService.connectMisWarehouseDb()
chattIntradaySolarForecastData = deficitCalculationService.fetchChattReForecast(startTargetTime, endTargetTime, 'SOLAR')
chattDayaheadSolarForecastData = deficitCalculationService.fetchChattReForecast(startDaTargetTime, endDaTargetTime, 'SOLAR')
deficitCalculationService.disconnectMisWarehouseDb()
intradaySolarForecastData.extend(chattIntradaySolarForecastData)
dayaheadSolarForecastData.extend(chattDayaheadSolarForecastData)

print("Fetched all forecast data successfully")
#fetching schedule data
intradaySdlDataDict = deficitCalculationService.fetchWbesSdlData(targetTime, allStateWbesAcr, scheduleRevNoDict['intRevNo'] )
daSdlDataDict = deficitCalculationService.fetchWbesSdlData(daTargetTime, allStateWbesAcr, scheduleRevNoDict['daRevNo'])

#Now calculating deficit using forecast, dc, sdl and re forecast
allStateIntradayDeficitData = pd.DataFrame()
allStateDayaheadDeficitData = pd.DataFrame()
for stateMetaData in deficitStateMetaData:
    stateScadaId = stateMetaData['stateScadaId']
    stateWbesAcr = stateMetaData['stateWbesAcr']
    stateRaDbAcr = stateMetaData['stateRaDbAcr']
    stateReAcr = stateMetaData['stateReAcr']
    stateIds = (stateScadaId, stateRaDbAcr, stateReAcr, stateWbesAcr)
    # Intraday deficit
    logger.info(f"intraday deficit calculating for {stateIds}")
    intradayStateDeficitDf = deficitCalculationService.calculateStateDeficit(
        intradayDemForecast,
        intradayDcData,
        intradaySolarForecastData,
        intradayWindForecastData,
        intradaySdlDataDict,
        'INTRADAY',
        stateIds,
        targetTime
    )
    allStateIntradayDeficitData = pd.concat([allStateIntradayDeficitData, intradayStateDeficitDf], ignore_index=True)
    #calculating deficit for dayahead
    logger.info(f"Dayahead deficit calculating for {stateIds}")
    dayaheadStateDeficitDf = deficitCalculationService.calculateStateDeficit(
        dayaheadDemForecast,
        daDcData,
        dayaheadSolarForecastData,
        dayaheadWindForecastData,
        daSdlDataDict,
        'DA',
        stateIds,
        daTargetTime
    )
    allStateDayaheadDeficitData = pd.concat([allStateDayaheadDeficitData, dayaheadStateDeficitDf], ignore_index=True)

# now checking if deficit is changed, if changed insert it as a new revision no
deficitCalculationService.connectRaPostgresDb()
latestIntradayRevision = deficitCalculationService.getLatestDeficitRevision(targetTime.date(),'INTRADAY')
latestDaRevision = deficitCalculationService.getLatestDeficitRevision(daTargetTime.date(),'DA')

logger.info("Fetching latest DA/Intraday deficit revision No")
# checking if latest revsion no for intraday/dayahead is none(means no revision stored in db) or has some value,
#  if it has some value we are checking if deficit changed
isDeficitChangedIntraday = False
isDeficitChangedDa = False
if latestIntradayRevision:
    isDeficitChangedIntraday = deficitCalculationService.checkDeficitValueChanged(allStateIntradayDeficitData, 'INTRADAY', latestIntradayRevision )
if latestDaRevision:
    isDeficitChangedDa = deficitCalculationService.checkDeficitValueChanged(allStateDayaheadDeficitData, 'DA', latestDaRevision )

if latestIntradayRevision and isDeficitChangedIntraday:
    # insert allStateIntradayDeficitData by increasing revision no
    incrementedRevision = deficitCalculationService.incrementRevisionNo(latestIntradayRevision)
    listOfRows = []
    for index, row in allStateIntradayDeficitData.iterrows():
        listOfRows.append((row['Timestamp'], row['State_Key'], row['Def_Type'], incrementedRevision, round(row['Def_Val']), round(row['Forecast_Val']), round(row['Sdl_Val']), round(row['DC_Val']), round(row['Wind_Fore_Val']), round(row['Solar_Fore_Val']), round(row['Others_Val'])))
    isInsertionSuccess = deficitCalculationService.insertStateDeficitData(listOfRows)
    if isInsertionSuccess:
        metaRecord= {"date": targetTime.date(),"time": targetTime.time(), "def_type": "INTRADAY","def_rev_no": incrementedRevision ,"forecast_rev_no": demForRevNoDict['intRevNo'], 
                     "sch_rev_no": scheduleRevNoDict['intRevNo'],"dc_rev_no": "INT1","reforecast_rev_no": reForeRevNoDict['intRevNo']}
        isInsertionSuccessForMetaData = deficitCalculationService.insertStateDeficitRevisionMetaData(metaRecord)
if latestDaRevision and isDeficitChangedDa:
    # insert allStateDayaheadDeficitData by increasing revision no
    incrementedRevision = deficitCalculationService.incrementRevisionNo(latestDaRevision)
    listOfRows = []
    for index, row in allStateDayaheadDeficitData.iterrows():
        listOfRows.append((row['Timestamp'], row['State_Key'], row['Def_Type'], incrementedRevision, round(row['Def_Val']), round(row['Forecast_Val']), round(row['Sdl_Val']), round(row['DC_Val']), round(row['Wind_Fore_Val']), round(row['Solar_Fore_Val']), round(row['Others_Val'])))
    isInsertionSuccess = deficitCalculationService.insertStateDeficitData(listOfRows)
    if isInsertionSuccess:
        metaRecord= {"date": daTargetTime.date(),"time": targetTime.time(), "def_type": "DA","def_rev_no": incrementedRevision ,"forecast_rev_no": demForRevNoDict['daRevNo'], 
                     "sch_rev_no": scheduleRevNoDict['daRevNo'], "dc_rev_no": "DA1","reforecast_rev_no": reForeRevNoDict['daRevNo']}
        isInsertionSuccessForMetaData = deficitCalculationService.insertStateDeficitRevisionMetaData(metaRecord)
if latestIntradayRevision is None:
    # means first Intraday revision of a day, no previous revision for a day, set revisionNo to INT1
    revisionNo = 'INT1'
    listOfRows = []
    for index, row in allStateIntradayDeficitData.iterrows():
        listOfRows.append((row['Timestamp'], row['State_Key'], row['Def_Type'], revisionNo, round(row['Def_Val']), round(row['Forecast_Val']), round(row['Sdl_Val']), round(row['DC_Val']), round(row['Wind_Fore_Val']), round(row['Solar_Fore_Val']), round(row['Others_Val'])))
    isInsertionSuccess = deficitCalculationService.insertStateDeficitData(listOfRows)
    if isInsertionSuccess:
        metaRecord= {"date": targetTime.date(),"time": targetTime.time(), "def_type": "INTRADAY","def_rev_no": revisionNo ,"forecast_rev_no": demForRevNoDict['intRevNo'], 
                     "sch_rev_no": scheduleRevNoDict['intRevNo'],"dc_rev_no": "INT1","reforecast_rev_no": reForeRevNoDict['intRevNo']}
        isInsertionSuccessForMetaData = deficitCalculationService.insertStateDeficitRevisionMetaData(metaRecord)
if latestDaRevision is None:
    # means first DA revision of a day, no previous revision for a day, set revision to DA1
    revisionNo = 'DA1'
    listOfRows = []
    for index, row in allStateDayaheadDeficitData.iterrows():
        listOfRows.append((row['Timestamp'], row['State_Key'], row['Def_Type'], revisionNo, round(row['Def_Val']), round(row['Forecast_Val']), round(row['Sdl_Val']), round(row['DC_Val']), round(row['Wind_Fore_Val']), round(row['Solar_Fore_Val']), round(row['Others_Val'])))
    isInsertionSuccess = deficitCalculationService.insertStateDeficitData(listOfRows)
    # isInsertionSuccess = deficitCalculationService.insertStateDeficitData(listOfRows)
    if isInsertionSuccess:
        metaRecord= {"date": daTargetTime.date(),"time": targetTime.time(), "def_type": "DA","def_rev_no": revisionNo ,"forecast_rev_no": demForRevNoDict['daRevNo'], 
                     "sch_rev_no": scheduleRevNoDict['daRevNo'], "dc_rev_no": "DA1","reforecast_rev_no": reForeRevNoDict['daRevNo']}
        isInsertionSuccessForMetaData = deficitCalculationService.insertStateDeficitRevisionMetaData(metaRecord)

deficitCalculationService.disconnectRaPostgresDb()    
logger.info("------------------All done--------------------------")