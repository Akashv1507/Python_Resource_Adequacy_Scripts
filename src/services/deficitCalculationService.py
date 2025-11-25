import datetime as dt
import requests
import json
import oracledb as cx_Oracle
import logging
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from src.appConfig import getAppConfig
import pandas as pd

# Create and configure logger
logging.basicConfig(filename="files/logs/pushDeficitData.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
# Creating an object
loggerr = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
loggerr.setLevel(logging.DEBUG)

class DeficitCalculationService:
    def __init__(self, postgresqlHost:str, postgresqlPort:int, postgresqlDbName:str, postgresqlUser:str, postgresqlPwd:str,conStringMisWarehouse:str,
                 wbesApiUrl:str, wbesRevNoUrl:str, WbesApiUser:str, WbesApiPass:str, wbesApikey:str ):
        self.postgresqlHost= postgresqlHost
        self.postgresqlPort= postgresqlPort
        self.postgresqlDbName= postgresqlDbName
        self.postgresqlUser= postgresqlUser
        self.postgresqlPwd= postgresqlPwd
        self.conStringMisWarehouse = conStringMisWarehouse
        self.wbesApiUrl= wbesApiUrl
        self.wbesRevNoUrl = wbesRevNoUrl
        self.WbesApiUser= WbesApiUser
        self.WbesApiPass = WbesApiPass
        self.wbesApikey= wbesApikey
        self.postgresqlConnection = None
        self.misWarehouseConnection = None

    def getDemandForecastRevNo(self, targetTime:dt.datetime):
        "return demand forecast rev no for targetTime {'daRevNo': 'R0A', 'intRevNo': 'R5'}"

        # Construct the datetime for today's 00:10, doing for intraday revsions
        startTime = targetTime.replace(hour=0, minute=10, second=0, microsecond=0)
        diff_minutes = (targetTime - startTime).total_seconds() / 60
        if diff_minutes < 0:
            return "Before R1"
        revisionIndex = int(diff_minutes // 90)

        #doing for dayahead revissions
        cuttOffDaRevTime = targetTime.replace(hour=22, minute=40, second=0, microsecond=0)
        daRevNo ="R0A"
        if targetTime>cuttOffDaRevTime:
            daRevNo= "R0B"

        return {'intRevNo': f"R{revisionIndex + 1}", 'daRevNo': daRevNo}

    def getReForecastRevNo(self, targetTime:dt.datetime):
        "return RE forecast rev no for targetTime {'daRevNo': 'DA_01', 'intRevNo': 'Rev_05'}"
       
        # Define first cutoff time: 01:02:00, doing for intraday revision, see photo of intraday revsion in RE forecasring
        firstCutoff = targetTime.replace(hour=1, minute=2, second=0, microsecond=0)
        interval = dt.timedelta(minutes=90)

        baseDatetime = firstCutoff
        intRevNo = 16  # Default to REV_16

        for i in range(15):  # Loop for REV_01 to REV_15
            if targetTime < baseDatetime:
                intRevNo = i + 1
                break
            baseDatetime += interval

        #doing for dayahead revissions
        daRevNo ="DA_1"
        cuttOffDa1RevTime = targetTime.replace(hour=0, minute=17, second=0, microsecond=0)
        cuttOffDa2RevTime = targetTime.replace(hour=5, minute=32, second=0, microsecond=0)
        cuttOffDa3RevTime = targetTime.replace(hour=14, minute=32, second=0, microsecond=0)
        cuttOffDa4RevTime = targetTime.replace(hour=22, minute=47, second=0, microsecond=0) 
        if targetTime>cuttOffDa1RevTime and targetTime<=cuttOffDa2RevTime:
            daRevNo ="DA_1"
        if targetTime>cuttOffDa2RevTime and targetTime<=cuttOffDa3RevTime:
            daRevNo ="DA_2"
        if targetTime>cuttOffDa3RevTime and targetTime<=cuttOffDa4RevTime:
            daRevNo ="DA_3"
        if targetTime>cuttOffDa4RevTime:
            daRevNo ="DA_4"
        return {'daRevNo': daRevNo, 'intRevNo': f"REV_{intRevNo:02d}"}

    def getLatestSchRevision(self, targetTime: dt.datetime, allRevisions: list) -> dict | None:
        latestRevision = None

        for revision in allRevisions:
            revisionTime = dt.datetime.strptime(revision["RevisionDateTimeStamp"], "%d-%m-%Y %H:%M:%S")
            if revisionTime <= targetTime:
                if (latestRevision is None) or (revisionTime > dt.datetime.strptime(latestRevision["RevisionDateTimeStamp"], "%d-%m-%Y %H:%M:%S")):
                    latestRevision = revision
        return latestRevision
    
    def getScheduleRevNO (self, targetTime:dt.datetime):
        "return wbes schedule rev no for targetTime {'daRevNo': '1', 'intRevNo': '5'}"

        intradayTargetStr= targetTime.strftime('%d-%m-%Y')
        daTargetTimeStr = (targetTime + dt.timedelta(days=1)).strftime('%d-%m-%Y')

        params = {"apikey": self.wbesApikey}
        headers = {"Content-Type": "application/json"}
        auth = (self.WbesApiUser,self.WbesApiPass)
        daBody = {"Date":daTargetTimeStr,"SchdRevNo": -1,"UserName":self.WbesApiUser, "UtilAcronymList": ["MSEB_State"], "UtilRegionIdList":[2]}   
        intradayBody = {"Date":intradayTargetStr,"SchdRevNo": -1,"UserName":self.WbesApiUser, "UtilAcronymList": ["MSEB_State"], "UtilRegionIdList":[2]}  
        daLatestRev={}
        intradayLatestRev={}
        # getting Da revision based on target time
        try:
            daResp = requests.post( self.wbesRevNoUrl, params=params, auth=auth, data=json.dumps(daBody), headers=headers)
            if not daResp.status_code == 200:
                print(daResp.status_code)
                print(f"unable to get da data from wbes api")
            respJson = daResp.json()
            allRevisionsList=respJson['ResponseBody']["AllRevisions"]
            daLatestRev= self.getLatestSchRevision(targetTime, allRevisionsList )
        except Exception as err:
                loggerr.exception(f'Error while making DA API call and err-> {err}')
                return []
        
        # getting Intraday revision based on target time
        try:
            intradayResp = requests.post( self.wbesRevNoUrl, params=params, auth=auth, data=json.dumps(intradayBody), headers=headers)
            if not intradayResp.status_code == 200:
                print(intradayResp.status_code)
                print(f"unable to get intraday data from wbes api")
            respJson = intradayResp.json()
            allRevisionsList=respJson['ResponseBody']["AllRevisions"]
            intradayLatestRev= self.getLatestSchRevision(targetTime, allRevisionsList )
        except Exception as err:
                loggerr.error(f'Error while making Intraday API call and err-> {err}')
                return []
        return {'daRevNo': daLatestRev["RevisionNo"], 'intRevNo': intradayLatestRev["RevisionNo"]}
        
    def connectMisWarehouseDb(self):
        """Establish a mis warehouse database connection."""
        if self.misWarehouseConnection:
            self.disconnectMisWarehouseDb()
        try:
            self.misWarehouseConnection = cx_Oracle.connect(self.conStringMisWarehouse)
            loggerr.info("Connected to Oracle MIS DB to Fetch Demand Forecast Data")
        except Exception as e:
            loggerr.error(f"MIS Oracle DB connection error: {str(e)}")
            self.misWarehouseConnection = None

    def disconnectMisWarehouseDb(self):
        """Close the mis warehouse database connection."""
        if self.misWarehouseConnection:
            loggerr.info("Closing Oracle (MIS) DB connection after fetching demand forecast")
            self.misWarehouseConnection.close()
            self.misWarehouseConnection = None

    def connectRaPostgresDb(self):
        """Establish a postgresql database connection."""
        if self.postgresqlConnection:
            self.disconnectRaPostgresDb()
        try:
            self.postgresqlConnection = psycopg.connect(dbname=self.postgresqlDbName,
                            user=self.postgresqlUser,
                            password=self.postgresqlPwd,
                            host=self.postgresqlHost,
                            port=self.postgresqlPort, row_factory=dict_row)
            loggerr.info("Connected to PostgreSql RA DB to Fetch DC data")
        except Exception as e:
            loggerr.error(f"RA postgresql Database connection error: {str(e)}")
            self.postgresqlConnection = None
    
    def disconnectRaPostgresDb(self):
        """Close the postgresql database connection."""
        if self.postgresqlConnection:
            loggerr.info("Closing Postgres (RA) DB connection after fetching DC data")
            try:
                self.postgresqlConnection.commit()
            except Exception as e:
                loggerr.warning(f"Postgres commit failed before closing: {str(e)}")
                self.postgresqlConnection.rollback()
            finally:
                self.postgresqlConnection.close()
                self.postgresqlConnection = None

    def fetchDemandForecast(self, startTimestamp: dt.datetime, endTimestamp: dt.datetime, entityTag: list, revisionNo: str):
        """
        Fetch data from the FORECAST_REVISION_STORE table based on timestamps, entity_tag, and revision_no.
        """
        if not self.misWarehouseConnection:
            loggerr.info("No active database MIS warehous connection.")
            return []
        # Dynamically build placeholders like :tag0, :tag1, :tag2
        loggerr.info(f"Demand Forecast Fetching Started for {startTimestamp.date()}/{endTimestamp.date()}/{revisionNo}")
        entityTagPlaceholders = ', '.join(f":tag{i}" for i in range(len(entityTag)))
        query = f"""
            SELECT TIME_STAMP,ENTITY_TAG, REVISION_NO, FORECASTED_DEMAND_VALUE 
            FROM FORECAST_REVISION_STORE 
            WHERE TIME_STAMP BETWEEN :start_ts AND :end_ts 
              AND ENTITY_TAG in ({entityTagPlaceholders})
              AND REVISION_NO = :revision_no
              order by ENTITY_TAG,TIME_STAMP
        """

        try:
            cursor = self.misWarehouseConnection.cursor()
            params ={
                "start_ts": startTimestamp,
                "end_ts": endTimestamp,
                "revision_no": revisionNo
            }
            # Add each tag as its own parameter, '
            params.update({f"tag{i}": tag for i, tag in enumerate(entityTag)})
            cursor.execute(query, params )

            rows = cursor.fetchall()

            # Process the result
            demandForecastData = [
                {
                    "timestamp": row[0],
                    "entityTag": row[1],
                    "revisionNo": row[2],
                    "forecastedDemandValue": row[3]
                }
                for row in rows
            ]

            return demandForecastData

        except Exception as e:
            loggerr.exception(f"Error during demand forecast data fetch: {str(e)}")
            return []
        finally:
            if cursor:
                cursor.close()
        
    def fetchStateDcData(self, revisionTypeTableName:str, startTime:dt.datetime, endTime:dt.datetime):
        """Fetch all state dc between starttime and endtime"""
        if not self.postgresqlConnection:
            loggerr.error("No active postgresql database connection.")
            return []
        loggerr.info(f"DC data fetching started for {revisionTypeTableName}/{startTime.date()}/{endTime.date()}")
        # Compose SQL with safe identifiers for table names
        query = sql.SQL("""select dadd.date_time , mt.state , sum(dadd.dc_data) dc_data
                            FROM {dc_table} dadd JOIN mapping_table mt ON dadd.plant_id = mt.id
                            where mt.id = dadd.plant_id and dadd.date_time between %(start_time)s AND %(end_time)s
                            group by mt.state , dadd.date_time
                            order by mt.state , dadd.date_time;
        """).format(dc_table=sql.Identifier(revisionTypeTableName))

        cursor = self.postgresqlConnection.cursor()

        try:
            cursor.execute(query, {"start_time": startTime,"end_time": endTime})
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            loggerr.exception(f"Error during Fetching DC data : {str(e)}")
            return []
        finally:
            if cursor:
                cursor.close()

    def fetchReForecastData(self, startTime:dt.datetime, endTime:dt.datetime, genType:str, reStateAcrList:list, revisionNo : str):
        """
        Fetch data from the REMC API
        """
        loggerr.info(f"RE forecast data ftech started for {startTime.date()}/{endTime.date()}/{genType}/{revisionNo}")
        appConfig = getAppConfig()
        remcApiBaseUrl= appConfig['remcApiBaseUrl']
        remcRevisionHistEndpoint= f"{remcApiBaseUrl}/api/hist/revisions"
        startTimeStr= startTime.strftime('%Y-%m-%d %H:%M:%S')
        endTimeStr= endTime.strftime('%Y-%m-%d %H:%M:%S')
        allstateReForecastData= []
        for stateAcr in reStateAcrList:
            try:
                response = requests.get(remcRevisionHistEndpoint, params={"startDatetime": startTimeStr, "endDatetime": endTimeStr, "stateCode": stateAcr, "genType": genType, "revisionNo": revisionNo, "fspID":"FSP00005" })
                if response.ok:
                    responseData= response.json()
                    stateReForecastData = responseData['responseData']
                    for item in stateReForecastData:
                        item["stateAcr"] = stateAcr
                    allstateReForecastData = allstateReForecastData + stateReForecastData
                else:
                    responseData= response.json()
                    print(responseData['responseData'])
            except Exception as err:
                print(f"API call failed with error -> {err}")
            
        return allstateReForecastData
    
    def fetchDaReForecastDataFromRealTimeApi(self, startTime:dt.datetime, endTime:dt.datetime, genType:str, reStateAcrList:list):
        """
        Fetch data from the REMC API, if DA RE Forecast data not found from revision store where all revisions are stored
        """
        loggerr.info(f"if DA RE Forecast data not found from revision store where all revisions are stored, fetch started for {startTime.date()}/{endTime.date()}/{genType}")
        appConfig = getAppConfig()
        remcApiBaseUrl= appConfig['remcApiBaseUrl']
        remcDayAheadApiEndpoint =f"{remcApiBaseUrl}/api/dayAhead"
        startTimeStr= startTime.strftime('%Y-%m-%d')
        endTimeStr= endTime.strftime('%Y-%m-%d')
        allstateDaReForecastData= []
        for stateAcr in reStateAcrList:
            try:
                response = requests.get(remcDayAheadApiEndpoint, params={"stateCode": stateAcr, "genType": genType, "startDate": startTimeStr, "endDate": endTimeStr})
                if response.ok:
                    responseData= response.json()
                    stateReForecasData =responseData['responseData'] 
                    for item in stateReForecasData:
                        item["stateAcr"] = stateAcr
                    allstateDaReForecastData= allstateDaReForecastData+ stateReForecasData
                    print(f"Api Fetch was success for {genType}/{stateAcr}/->{responseData['responseText']}")
                else:
                    responseData= response.json()
                    print(f"Api Fetch was aborted for {genType}/{stateAcr}->{responseData['responseText']}")
            except Exception as err:
                print(f"API call failed with error -> {err}")
        return allstateDaReForecastData    
    
    def fetchWbesSdlData(self, fetchDate:dt.datetime, stateWbesAcrList:list, revNo:str):
        """ Fetch WBES SDL for specific date and revision no"""
        appConfig = getAppConfig()
        wbesApiUrl= appConfig['wbesApiUrl']
        wbesApikey= appConfig['wbesApikey']
        WbesApiUser= appConfig['WbesApiUser']
        WbesApiPass= appConfig['WbesApiPass']
        startTimeStr= fetchDate.strftime('%d-%m-%Y')
        loggerr.info(f"WBES Sdl getch started for {startTimeStr}/{revNo}")
        wbesApiParams = {"apikey": wbesApikey}
        wbesApiHeaders = {"Content-Type": "application/json"}
        auth = (WbesApiUser, WbesApiPass)
        body = {"Date":startTimeStr,"SchdRevNo": revNo,"UserName":WbesApiUser, "UtilAcronymList": stateWbesAcrList,"UtilRegionIdList":[2]}
        groupwiseSdlData= []
        allStateSdlDataDict= {}
        try:
            response = requests.post(wbesApiUrl, params=wbesApiParams, auth=auth, data=json.dumps(body), headers=wbesApiHeaders)
            if not response.status_code == 200:
                print(response.status_code)
                print(f"unable to get data from wbes api")
            wbesApiRespData = response.json()
            groupwiseSdlData = wbesApiRespData['ResponseBody']["GroupWiseDataList"]
        except Exception as err:
            print(f"Api Error with error = {err}")

        for stateGrpWiseData in groupwiseSdlData:
            stateAcr = stateGrpWiseData['Acronym']
            stateSdlList = stateGrpWiseData['NetScheduleSummary']['TotalNetSchdAmount']
            allStateSdlDataDict[stateAcr]= stateSdlList

        return allStateSdlDataDict
    
    def calculateStateDeficit(self, forecastData, dcData, solarData, windData, sdlDataDict, defType, stateIds, targetDate):
        """ calculating single state DA/Intraday deficit for specific data"""
        stateScadaId, stateRaDbAcr, stateReAcr, stateWbesAcr = stateIds
        # Generate 96 timestamps at 15-minute intervals
        timestampList = pd.date_range(
            start=pd.Timestamp(targetDate).replace(hour=0, minute=0, second=0, microsecond=0),
            periods=96,
            freq='15min'
        ).tolist()

        forecastList = []
        sdlList = []
        dcList = []
        windForList = []
        solarForList = []
        othersList = []
        defTypeList = [defType] * 96

        # Forecast Data
        for data in forecastData:
            if data['entityTag'] == stateScadaId:
                forecastList.append(data['forecastedDemandValue'])

        # DC Data
        for data in dcData:
            if data['state'] == stateRaDbAcr:
                    dcList.append(data['dc_data'])

       
        # Solar Data
        for data in solarData:
            if data['stateAcr'] == stateReAcr:
                solarForList.append(float(data['value']))

        # Wind Data
        for data in windData:
            if data['stateAcr'] == stateReAcr:
                windForList.append(float(data['value']))

        # SDL Data
        sdlList = sdlDataDict.get(stateWbesAcr, [])

        # Pad all lists to length 96
        def pad_to_96(lst):
            return lst + [0] * (96 - len(lst)) if len(lst) < 96 else lst[:96]

        forecastList = pad_to_96(forecastList)
        sdlList = pad_to_96(sdlList)
        dcList = pad_to_96(dcList)
        windForList = pad_to_96(windForList)
        solarForList = pad_to_96(solarForList)
        othersList = [0] * 96  # Always zero or customize as needed

        # Calculate Deficit
        defValList = [
            forecastList[i] - (
                sdlList[i] + dcList[i] + windForList[i] + solarForList[i] + othersList[i]
            )
            for i in range(96)
        ]

        # Create DataFrame
        deficitData = pd.DataFrame({
            'Timestamp': timestampList,
            'State_Key': [stateRaDbAcr] * 96,
            'Def_Type': defTypeList,
            'Def_Val': defValList,
            'Forecast_Val': forecastList,
            'Sdl_Val': sdlList,
            'DC_Val': dcList,
            'Wind_Fore_Val': windForList,
            'Solar_Fore_Val': solarForList,
            'Others_Val': othersList
        })

        return deficitData
    
    def getLatestDeficitRevision(self, dateKey:dt.date, defType:str ):
        """geting latest revision no for particular date and deficit type, return None for no revision for particular date"""
        
        if not self.postgresqlConnection:
            print("No active postgresql database connection.")
            return []

        # Compose SQLregexp_replace(def_rev_no, '^[^0-9]*', ''): removes all non-numeric characters from the start of the string, leaving only the numeric part
        query = sql.SQL(""" SELECT def_rev_no
                            FROM deficit_revision_metadata
                            WHERE date = %(date_key)s AND def_type = %(def_type)s
                            ORDER BY (regexp_replace(def_rev_no, '^[^0-9]*', ''))::int DESC
                            LIMIT 1; """)
        cursor = self.postgresqlConnection.cursor()
        latestRev= None
        try:
            cursor.execute(query, {"date_key": dateKey,"def_type": defType})
            result = cursor.fetchone()
            # result = ('INT_1',)
            if result:
                latestRev = result['def_rev_no']
            return latestRev
        except Exception as e:
            print(f"Error fetching latest revision from deficit revision meta data: {str(e)}")
            return latestRev
        finally:
            if cursor:
                cursor.close()
        
    def checkDeficitValueChanged (self, deficitDataDf:pd.DataFrame, defType:str, defRevNo:str):
        """ checking if deficit value changed from calculated deficit to latest deficit stored in db"""
        if not self.postgresqlConnection:
            print("No active postgresql database connection.")
            return []
        # Create a list of (timestamp, state_key) tuples from the DataFrame
        lookupKeys = list(deficitDataDf[['Timestamp', 'State_Key']].itertuples(index=False, name=None))
        # Build the placeholders for the IN clause
        placeholders = ', '.join(['(%s, %s)'] * len(lookupKeys))
        # Flatten the tuple list to pass as parameters
        flattenedValues = [item for tup in lookupKeys for item in tup]
        # Final query
        query = f"""
            SELECT timestamp, state_key, def_val
            FROM state_deficit_data
            WHERE def_type = %s AND def_revision_no = %s
            AND (timestamp, state_key) IN ({placeholders})
        """
        #  Step 5: Combine all query parameters: [def_type, def_rev_no, ts1, sk1, ts2, sk2, ...]
        queryParams = [defType, defRevNo] + flattenedValues
        cursor = self.postgresqlConnection.cursor()
        # setting this to true so that no new revision inserted if there is connection error
        isChanged = False
        try:
            cursor.execute(query, queryParams)            
            rows = cursor.fetchall()
            # Convert DB results to a DataFrame
            dbDf = pd.DataFrame(rows)
            # Rename dbDf columns to match deficitDataDf
            dbDf.rename(columns={'timestamp': 'Timestamp','state_key': 'State_Key','def_val': 'Def_Val'}, inplace=True)
            # Merge DataFrames on Timestamp and State_Key
            mergedDf = pd.merge(
                deficitDataDf,
                dbDf,
                how='inner',
                left_on=['Timestamp', 'State_Key'],
                right_on=['Timestamp', 'State_Key'],
                suffixes=('_df', '_db')
            )

            # Compute absolute difference
            mergedDf['diff'] = (mergedDf['Def_Val_df'] - mergedDf['Def_Val_db']).abs()

            # Check if all differences are <= 50
            isWhithinMargin = (mergedDf['diff'] <= 50).all()
            isChanged = not isWhithinMargin
            return isChanged
        except Exception as e:
            print(f"Error fetching latest revision data : {str(e)}")
            return isChanged
        finally:
            if cursor:
                cursor.close()

    def insertStateDeficitData(self, records:list,  ):
        """Insert state deficit data present in records variable to postgresql db."""
        if not self.postgresqlConnection:
            print("No active database connection.")
            return False
        isInsertionSuccessfull= False
        cursor = self.postgresqlConnection.cursor()
        insertQuery = """
        INSERT INTO public.state_deficit_data (
        timestamp, state_key, def_type, def_revision_no,
        def_val, forecast_val, sdl_val, dc_val,
        wind_fore_val, solar_fore_val, others_val
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (timestamp, state_key, def_type, def_revision_no) DO NOTHING
        """
        try:
            cursor.executemany(insertQuery, records)
            self.postgresqlConnection.commit()
            isInsertionSuccessfull= True
        except Exception as e:
            print(f"Error pushing state deficit  data: {str(e)}")
        finally:
            if cursor:
                cursor.close()
            return isInsertionSuccessfull

    def incrementRevisionNo(self, label: str) -> str:
        # Split prefix and numeric part and increment revision no
        i = 0
        while i < len(label) and not label[i].isdigit():
            i += 1

        prefix = label[:i]
        number_part = label[i:]

        if number_part.isdigit():
            incremented_number = int(number_part) + 1
            return f"{prefix}{incremented_number}"
        else:
            return label  # return as-is if no digits found

    def insertStateDeficitRevisionMetaData(self, record:list,  ):
        """Insert state deficit revision data present in record variable to postgresql db."""
        if not self.postgresqlConnection:
            print("No active database connection.")
            return False
        isInsertionSuccessfull= False
        cursor = self.postgresqlConnection.cursor()
        # Insert query with placeholders
        insert_query = """
            INSERT INTO public.deficit_revision_metadata (
                date, time, def_type, def_rev_no,
                forecast_rev_no, sch_rev_no, dc_rev_no, reforecast_rev_no
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, def_type, def_rev_no) DO NOTHING
        """
        try:
            # Execute insert
            cursor.execute(insert_query, (
                record["date"], record["time"], record["def_type"], record["def_rev_no"],
                record["forecast_rev_no"], record["sch_rev_no"],
                record["dc_rev_no"], record["reforecast_rev_no"]
            ))
            self.postgresqlConnection.commit()
            isInsertionSuccessfull= True
        except Exception as e:
            print(f"Error pushing state deficit  data: {str(e)}")
        finally:
            if cursor:
                cursor.close()
            return isInsertionSuccessfull

    def fetchChattReForecast(self, startTimestamp: dt.datetime, endTimestamp: dt.datetime, reType: str):
        """
        Fetch RE data from the RE_FORECAST table based on timestamps, entity_tag, and re_type.
        """
        if not self.misWarehouseConnection:
            loggerr.info("No active database MIS warehous connection.")
            return []
        # Dynamically build placeholders like :tag0, :tag1, :tag2
        loggerr.info(f"Chatt RE Forecast Fetching Started for {startTimestamp.date()}/{endTimestamp.date()}/{reType}")
        query = f"""
            SELECT TIME_STAMP, RE_FORECASTED_VALUE 
            FROM RE_FORECAST 
            WHERE TIME_STAMP BETWEEN :start_ts AND :end_ts 
              AND ENTITY_TAG = 'WRLDCMP.SCADA1.A0046945'
              AND Re_TYPE = :re_type
              order by TIME_STAMP
        """

        try:
            cursor = self.misWarehouseConnection.cursor()
            params ={
                "start_ts": startTimestamp,
                "end_ts": endTimestamp,
                "re_type": reType
            }
           
            cursor.execute(query, params )

            rows = cursor.fetchall()

            # Process the result
            reForecastData = [
                {
                    "timestamp": row[0].strftime('%Y-%m-%d %H:%M'),
                    "value": row[1],
                    "stateAcr": "IN_CH"
                }
                for row in rows
            ]
            return reForecastData

        except Exception as e:
            loggerr.exception(f"Error during demand forecast data fetch: {str(e)}")
            return []
        finally:
            if cursor:
                cursor.close()