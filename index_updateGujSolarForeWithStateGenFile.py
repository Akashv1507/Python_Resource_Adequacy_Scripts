import datetime as dt
import logging
from src.appConfig import loadAppConfig
import pandas as pd
from src.reForecastDataInsertion import ReForecastInsertion
from cryptography.hazmat.primitives.kdf import pbkdf2


# Create and configure logger
logging.basicConfig(filename="files/logs/pushReForecast.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


appConfig = loadAppConfig()
dbConStr = appConfig['con_string_mis_warehouse']
finalDumpFolderPath = appConfig['finalDumpFolderPath']

currentDate = dt.datetime.now()
currentDate=currentDate.replace(hour=0, minute=0, second=0, microsecond=0)
currDateMinus1=currentDate-dt.timedelta(days=1)
currDateMinus2=currentDate-dt.timedelta(days=2)
currDateMinus3=currentDate-dt.timedelta(days=3)

timestampList=[]

obj_reForecastInsertion=ReForecastInsertion(dbConStr)

logger.info("------Gujarat Solar Forecast Updation Started From STATE GEN SCADA file----------")

def generateSolarForecastValList(dateKey:dt.datetime):
    
    solarForecastValList=[]
    dateStr = dateKey.strftime('%d_%m_%Y')
    # State_Gen_08_05_2024
    stateGenFilePath =   f'{finalDumpFolderPath}State_Gen_{dateStr}.xlsx' 
    dateparse = lambda x: dt.datetime.strptime(x, "%m-%d-%Y %H:%M:%S")
    try:
        gujSolarStateGenDf = pd.read_excel(stateGenFilePath, skiprows=2, usecols = ["Timestamp","GUJ_SOLAR"], nrows=1440, parse_dates=['Timestamp']) 
        gujSolarStateGenDf.set_index('Timestamp', inplace=True)
        gujSolarStateGenDf= gujSolarStateGenDf.resample('15T').mean()
        gujSolarStateGenDf.reset_index(drop=True, inplace=True)
        for ind in gujSolarStateGenDf.index:
           solarForecastValList.append(gujSolarStateGenDf['GUJ_SOLAR'][ind])
        return solarForecastValList
    except Exception as err:
        logger.error(f"Error while reading state Gen file for {dateStr} with {err}")
        return [0 for i in range(1, 97)]


currDateMinus1SolarForecastValList = generateSolarForecastValList(currDateMinus1)
currDateMinus2SolarForecastValList = generateSolarForecastValList(currDateMinus2)
currDateMinus3SolarForecastValList = generateSolarForecastValList(currDateMinus3)

threeDaysAvgSolarForecastValList= [ ( currDateMinus1SolarForecastValList[i] + currDateMinus2SolarForecastValList[i] + currDateMinus3SolarForecastValList[i])/3 for i in range(0,96)]
todayTommCombinedSolarForecastValList= threeDaysAvgSolarForecastValList*2
loopStartTime= currentDate
loopEndTime= currentDate+dt.timedelta(days=2)
while loopStartTime<loopEndTime:
    timestampList.append(loopStartTime.strftime('%d-%m-%Y %H:%M:%S'))
    loopStartTime=loopStartTime+dt.timedelta(minutes=15)

solarForecastValDf= pd.DataFrame()
solarForecastValDf['Time_Stamp']=timestampList
solarForecastValDf['Entity_Tag']="WRLDCMP.SCADA1.A0046957"
solarForecastValDf['RE_Forecasted_Value'] = todayTommCombinedSolarForecastValList
solarForecastValDf['RE_Type'] = 'SOLAR'
msgIntraday = obj_reForecastInsertion.pushDataToDb(solarForecastValDf)
logger.info(msgIntraday)





        