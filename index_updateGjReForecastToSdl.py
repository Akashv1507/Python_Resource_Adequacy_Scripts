import datetime as dt
import logging
from src.appConfig import loadAppConfig
import requests
import json
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
wbesApiUser = appConfig['wbesApiUserName']
wbesApiPass = appConfig['wbesApiPass']

currentDate = dt.datetime.now()
tommDate = dt.datetime.now() + dt.timedelta(days=1)
currentDate=currentDate.replace(hour=0, minute=0, second=0, microsecond=0)
tommDate=tommDate.replace(hour=0, minute=0, second=0, microsecond=0)

obj_reForecastInsertion=ReForecastInsertion(dbConStr)

wbesUtilsUrl= 'https://wbes.wrldc.in/ReportFullSchedule/GetUtils?regionId=2'
solarGenAcrList=[]
windGenAcrList=[]

logger.info("----------Gujarat Schedule Updation Started--------------")

def generateDf(dateKey:dt.datetime, solarGenAcrList:list, windGenAcrList:list ):
    
    timestampList=[]
    dateStr = dateKey.strftime('%d-%m-%Y')
    currDayWbesApiUrl = f"https://wbes.wrldc.in/WebAccess/GetFilteredSchdData?USER={wbesApiUser}&PASS={wbesApiPass}&DATE={dateStr}&ACR=GEB_State"
    loopStartTime= dateKey
    loopEndTime= dateKey+dt.timedelta(days=1)
    while loopStartTime<loopEndTime:
        timestampList.append(loopStartTime.strftime('%d-%m-%Y %H:%M:%S'))
        loopStartTime=loopStartTime+dt.timedelta(minutes=15)
    solarSdlDf= pd.DataFrame()
    windSdlDf = pd.DataFrame()
    windSolarSumSdlDf= pd.DataFrame()
    solarSdlDf['Time_Stamp']=timestampList
    solarSdlDf['Entity_Tag']="WRLDCMP.SCADA1.A0046957"
    windSdlDf['Time_Stamp']=timestampList
    windSdlDf['Entity_Tag']="WRLDCMP.SCADA1.A0046957"
    
    try:
        response = requests.get(currDayWbesApiUrl)
        wbesApiRespData = json.loads(response.text)
        fullSdlList = wbesApiRespData['groupWiseDataList'][0]['fullschdList']
        for genSdl in fullSdlList:
            sellerAcr = genSdl['SellerAcr']
            if sellerAcr in solarGenAcrList:
                solarSdlDf[sellerAcr]= [ float(numb) for numb in genSdl['ScheduleAmount'].split(',')]
            if sellerAcr in windGenAcrList:
                windSdlDf[sellerAcr]= [ float(numb) for numb in genSdl['ScheduleAmount'].split(',')]
        solarSdlDf['RE_Forecasted_Value'] = solarSdlDf.sum(numeric_only=True, axis=1)
        solarSdlDf['RE_Type'] = 'SOLAR'
        windSdlDf['RE_Forecasted_Value'] = windSdlDf.sum(numeric_only=True, axis=1)
        windSdlDf['RE_Type'] = 'WIND'
        
        windSolarSumSdlDf = pd.concat([solarSdlDf[['Time_Stamp', 'Entity_Tag','RE_Type','RE_Forecasted_Value']], windSdlDf[['Time_Stamp', 'Entity_Tag','RE_Type','RE_Forecasted_Value']]])
        windSolarSumSdlDf.reset_index(drop=True, inplace=True)
        return windSolarSumSdlDf
        
    except Exception as err:
        logger.error(f"Error while making wbes api call= {err}")
        return None
try:
    response = requests.get(wbesUtilsUrl, headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0'})
    wbesApiRespData = json.loads(response.text)
    sellersList = wbesApiRespData['sellers']
    for seller in sellersList:
        if seller['IsgsTypeId']== 5 and seller['IsgsSubTypeId']== 1:
           solarGenAcrList.append(seller['Acronym'])
        if seller['IsgsTypeId']== 5 and seller['IsgsSubTypeId']== 2:
            windGenAcrList.append(seller['Acronym'])
    logger.info(f"Solar Gen List {solarGenAcrList} ")
    logger.info(f"Wind Gen List {windGenAcrList} ")
except Exception as err:
    logger.error(f"Gen Utils  processing Error with error = {err}")
    exit()

# for gujarat RE forecast is wrong, hence taking schedule values
intradaySdlDf = generateDf(currentDate, solarGenAcrList, windGenAcrList)
dayAheadSdlDf = generateDf(tommDate, solarGenAcrList, windGenAcrList)
msgIntraday = obj_reForecastInsertion.pushDataToDb(intradaySdlDf)
logger.info(msgIntraday)
msgDayAhead = obj_reForecastInsertion.pushDataToDb(dayAheadSdlDf)
logger.info(msgDayAhead)




        