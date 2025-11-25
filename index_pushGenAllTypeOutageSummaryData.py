from src.appConfig import loadAppConfig
import datetime as dt
import argparse
import logging
import pandas as pd
from cryptography.hazmat.primitives.kdf import pbkdf2
from src.services.genAllTypeOutagesSummaryService import GenAllTypeOutageSummaryService

# Create and configure logger
logging.basicConfig(filename="files/logs/pushGenAllTypeOutageSummary.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
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
logger.info(f"------Insertion started of outage summary b/w {dt.datetime.strftime(startDateTime, '%Y-%m-%d')}To{dt.datetime.strftime(endDateTime, '%Y-%m-%d')}--------- ")
genAllTypeOutageSummaryService = GenAllTypeOutageSummaryService(appConfig["postgresqlHost"], appConfig["postgresqlPort"], appConfig["postgresqlDb"], appConfig["postgresqlUser"], appConfig["postgresqlPass"], appConfig["con_string_outage_db"], appConfig['instantClientPath'])


# fetching actual gen all type outage data
outageSummaryAllRows = []
combinedOutageSummaryAllTime = pd.DataFrame()
currdatetime = startDateTime
genAllTypeOutageSummaryService.connectOutgaeDb()
try:
    while currdatetime<=endDateTime: 
        outageDf = genAllTypeOutageSummaryService.fetchGenAllTypeOutageData(currdatetime)
        outageDf.insert(0, 'TIME_STAMP', currdatetime)
        # outageDf['CLASSIFICATION'] = outageDf['CLASSIFICATION'].replace('STATE_IPP', 'STATE_OWNED')
        combinedOutageSummaryAllTime = pd.concat([combinedOutageSummaryAllTime, outageDf], axis=0, ignore_index=True)
        currdatetime = currdatetime + dt.timedelta(minutes=15)
except Exception as err:
    logger.exception(f"error in main file - {err}")
genAllTypeOutageSummaryService.disconnectOutgaeDb()
outageSummaryAllRows = combinedOutageSummaryAllTime[['TIME_STAMP','STATE_NAME','CLASSIFICATION','STATION_TYPE','SHUT_DOWN_TYPE_NAME','SHUTDOWN_TAG','OUTAGE_VAL']].values.tolist()
# inserting outageSummary Rows
if len(outageSummaryAllRows)>0:
    logger.info(f"Total {len(outageSummaryAllRows)} rows to be inserted in outage_details table for gen all type outage summary")
    genAllTypeOutageSummaryService.connectRaPostgresDb()
    genAllTypeOutageSummaryService.insertGenAllTypeOutageSummaryRows(outageSummaryAllRows)
    genAllTypeOutageSummaryService.disconnectRaPostgresDb()



