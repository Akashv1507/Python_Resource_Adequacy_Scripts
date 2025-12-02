from src.appConfig import loadAppConfig
import datetime as dt
import argparse
import logging
import pandas as pd
from cryptography.hazmat.primitives.kdf import pbkdf2
from src.services.outageSummaryService import OutageSummaryService

# Create and configure logger
logging.basicConfig(filename="files/logs/pushOutageSummary.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


appConfig = loadAppConfig()
raStateMapping = appConfig['raStateMapping']
raFuelMapping = appConfig['raFuelMapping']

# Get current time, script will run each hour, while pushing last 2 hours data ending at nearest completed block
now = dt.datetime.now()
# Round down to the nearest 15 minutes
minute = (now.minute // 15) * 15
roundedNow = now.replace(minute=0, second=0, microsecond=0) + dt.timedelta(minutes=minute)
# Set endDateTime and startDateTime
endDateTime = roundedNow
startDateTime = endDateTime - dt.timedelta(hours=2) 

# get start and end dates from command line
parser = argparse.ArgumentParser()
parser.add_argument('--start_dateTime', help="Enter Start date in yyyy-mm-dd format",
                    default=dt.datetime.strftime(startDateTime, '%Y-%m-%d %H:%M:%S'))
parser.add_argument('--end_dateTime', help="Enter end date in yyyy-mm-dd format",
                    default=dt.datetime.strftime(endDateTime, '%Y-%m-%d %H:%M:%S'))                   
args = parser.parse_args()
startDateTime = dt.datetime.strptime(args.start_dateTime, '%Y-%m-%d %H:%M:%S')
endDateTime = dt.datetime.strptime(args.end_dateTime, '%Y-%m-%d %H:%M:%S')
# startDateTime = startDateTime.replace(hour=0, minute=0, second=0, microsecond=0)
# endDateTime = endDateTime.replace(hour=0, minute=0, second=59, microsecond=0)
validStationTypesList = ['THERMAL', 'GAS', 'HYDEL']
logger.info(f"------Insertion started of outage summary b/w {dt.datetime.strftime(startDateTime, '%Y-%m-%d %H:%M:%S')}To{dt.datetime.strftime(endDateTime, '%Y-%m-%d %H:%M:%S')}--------- ")
outageSummaryService = OutageSummaryService(appConfig["postgresqlHost"], appConfig["postgresqlPort"], appConfig["postgresqlDb"], appConfig["postgresqlUser"], appConfig["postgresqlPass"], appConfig["con_string_outage_db"], appConfig['instantClientPath'])

# fetching meta data for generating units
outageSummaryService.connectOutgaeDb()
stateInstCapacityMetaDataDf = outageSummaryService.fetchGenMetaData()
outageSummaryService.disconnectOutgaeDb()

# fetching Day-Ahead , Intraday Dc data for states.
outageSummaryService.connectRaPostgresDb()
daDcDataDf = outageSummaryService.fetchStateDcData("day_ahead_dc_data", startDateTime, endDateTime)
intradayDcDataDf = outageSummaryService.fetchStateDcData("intraday_dc_data", startDateTime, endDateTime)
outageSummaryService.disconnectRaPostgresDb()

# fetching actual outage data
outageSummaryAllRows = []
currdatetime = startDateTime
outageSummaryService.connectOutgaeDb()
try:
    while currdatetime<=endDateTime: 
        outageDf = outageSummaryService.fetchOutageData(currdatetime)
        outageDf['CLASSIFICATION'] = outageDf['CLASSIFICATION'].replace('STATE_IPP', 'STATE_OWNED')
        # Group by required fields and sum the installed capacity
        outageCapacitySummaryDf = (
            outageDf.groupby(['STATE_NAME', 'CLASSIFICATION', 'STATION_TYPE'])['INSTALLED_CAPACITY']
            .sum()
            .reset_index()
            .sort_values(by='STATE_NAME', ascending=False)
        )
        # iterating over all state insalled capacity rows and filtering outage capacity df based on current all state rows on basis of name, classification, type
        for indexMetaInstCap, rowMetaInstCap in stateInstCapacityMetaDataDf.iterrows():
            stateName = rowMetaInstCap['STATE_NAME']
            stationType = rowMetaInstCap['STATION_TYPE']
            classification = rowMetaInstCap['CLASSIFICATION']
            installedCapacity = rowMetaInstCap['INSTALLED_CAPACITY']
            outageCapacity = 0
            intradayDcofBlk = 0
            daDcOfBlk = 0
            normDc = 0
            if classification == 'STATE_OWNED' and stationType in validStationTypesList and stateName is not None:
                filteredOutageCapacitySummaryDf = outageCapacitySummaryDf[(outageCapacitySummaryDf['STATE_NAME']==stateName) & (outageCapacitySummaryDf['STATION_TYPE']== stationType) & (outageCapacitySummaryDf['CLASSIFICATION']== classification)]
                if not filteredOutageCapacitySummaryDf.empty:
                        outageCapacity= filteredOutageCapacitySummaryDf['INSTALLED_CAPACITY'].iloc[0]
                # getting total DC of particular state,classification, stationType of particular TB from RA db
                # Get RA Mappings
                raStateName = raStateMapping.get(stateName)
                raFuelType = raFuelMapping.get(stationType)

                if raStateName and raFuelType:
                    # Filter Intraday DC
                    if not intradayDcDataDf.empty:
                        filteredIntradayDcDataDf = intradayDcDataDf[(intradayDcDataDf['date_time'] == currdatetime) &(intradayDcDataDf['state'] == raStateName) &(intradayDcDataDf['fuel_type'] == raFuelType)]
                        if not filteredIntradayDcDataDf.empty:
                            intradayDcofBlk = filteredIntradayDcDataDf['dc_data'].iloc[0]

                    # Filter DA DC
                    if not daDcDataDf.empty:
                        filteredDaDcDataDf = daDcDataDf[(daDcDataDf['date_time'] == currdatetime) &(daDcDataDf['state'] == raStateName) &(daDcDataDf['fuel_type'] == raFuelType)]
                        if not filteredDaDcDataDf.empty:
                            daDcOfBlk = filteredDaDcDataDf['dc_data'].iloc[0]

                    # Calculate normDc and outages
                    normDc = (installedCapacity - outageCapacity) * 0.93
                    # intPartialOutage = normDc - intradayDcofBlk
                    # daPartialOutage = normDc - daDcOfBlk
                    outageSummaryAllRows.append((currdatetime, raStateName, raFuelType, outageCapacity, normDc, intradayDcofBlk, daDcOfBlk ))  
        currdatetime = currdatetime + dt.timedelta(minutes=15)
except Exception as err:
    logger.exception(f"error in main file - {err}")

outageSummaryService.disconnectOutgaeDb()

# inserting outageSummary Rows
outageSummaryService.connectRaPostgresDb()
outageSummaryService.insertOutageSummaryRows(outageSummaryAllRows)
outageSummaryService.disconnectRaPostgresDb()



