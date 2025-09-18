@REM pyinstaller index_pushForecastData.py --onefile
@REM pyinstaller index_generateDcSdlActDataForScada.py --onefile
pyinstaller index_pushDaReForecastFromRemcApi.py --onefile
pyinstaller index_pushIntradayReForecastFromRemcApiRealtime.py --onefile
@REM pyinstaller index_pushChattSolarForecastData.py --onefile



@REM xcopy /y dist\index_pushForecastData.exe index_pushForecastData.exe*
@REM xcopy /y dist\index_generateDcSdlActDataForScada.exe index_generateDcSdlActDataForScada.exe*
xcopy /y dist\index_pushDaReForecastFromRemcApi.exe index_pushDaReForecastFromRemcApi.exe*
xcopy /y dist\index_pushIntradayReForecastFromRemcApiRealtime.exe index_pushIntradayReForecastFromRemcApiRealtime.exe*
@REM xcopy /y dist\index_pushChattSolarForecastData.exe index_pushChattSolarForecastData.exe*
