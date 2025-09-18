@REM pyinstaller index_pushForecastData.py --onefile
@REM pyinstaller index_generateDcSdlActDataForScada.py --onefile
@REM pyinstaller index_downloadRemcDaForecastExcel.py --onefile
@REM pyinstaller index_pushDaReForecastData.py --onefile
@REM pyinstaller index_downloadRemcIntradayForecastExcel.py --onefile
@REM pyinstaller index_pushIntradayReForecastData.py --onefile
@REM pyinstaller index_updateGjReForecastToSdl.py --onefile
@REM pyinstaller index_pushChattSolarForecastData.py --onefile
@REM pyinstaller index_updateGujSolarForeWithStateGenFile.py --onefile
@REM pyinstaller index_pushLoadSheddingData.py --onefile
pyinstaller index_pushOutageSummaryData.py --onefile
@REM pyinstaller index_pushStateDeficitData.py --onefile

@REM xcopy /y dist\index_pushForecastData.exe index_pushForecastData.exe*
@REM xcopy /y dist\index_generateDcSdlActDataForScada.exe index_generateDcSdlActDataForScada.exe*
@REM xcopy /y dist\index_downloadRemcDaForecastExcel.exe index_downloadRemcDaForecastExcel.exe*
@REM xcopy /y dist\index_pushDaReForecastData.exe index_pushDaReForecastData.exe*
@REM xcopy /y dist\index_downloadRemcIntradayForecastExcel.exe index_downloadRemcIntradayForecastExcel.exe*
@REM xcopy /y dist\index_pushIntradayReForecastData.exe index_pushIntradayReForecastData.exe*
@REM xcopy /y dist\index_updateGjReForecastToSdl.exe index_updateGjReForecastToSdl.exe*
@REM xcopy /y dist\index_pushChattSolarForecastData.exe index_pushChattSolarForecastData.exe*
@REM xcopy /y dist\index_updateGujSolarForeWithStateGenFile.exe index_updateGujSolarForeWithStateGenFile.exe*
@REM xcopy /y dist\index_pushLoadSheddingData.exe index_pushLoadSheddingData.exe*
xcopy /y dist\index_pushOutageSummaryData.exe index_pushOutageSummaryData.exe*
@REM xcopy /y dist\index_pushStateDeficitData.exe index_pushStateDeficitData.exe*