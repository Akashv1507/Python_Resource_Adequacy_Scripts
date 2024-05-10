pyinstaller index_pushForecastData.py --onefile
pyinstaller index_generateDcSdlActDataForScada.py --onefile
pyinstaller index_downloadRemcDaForecastExcel.py --onefile
pyinstaller index_pushDaReForecastData.py --onefile
pyinstaller index_downloadRemcIntradayForecastExcel.py --onefile
pyinstaller index_pushIntradayReForecastData.py --onefile
pyinstaller index_updateGjReForecastToSdl.py --onefile
pyinstaller index_pushChattSolarForecastData.py --onefile
pyinstaller index_updateGujSolarForeWithStateGenFile.py --onefile

xcopy /y dist\index_pushForecastData.exe index_pushForecastData.exe*
xcopy /y dist\index_generateDcSdlActDataForScada.exe index_generateDcSdlActDataForScada.exe*
xcopy /y dist\index_downloadRemcDaForecastExcel.exe index_downloadRemcDaForecastExcel.exe*
xcopy /y dist\index_pushDaReForecastData.exe index_pushDaReForecastData.exe*
xcopy /y dist\index_downloadRemcIntradayForecastExcel.exe index_downloadRemcIntradayForecastExcel.exe*
xcopy /y dist\index_pushIntradayReForecastData.exe index_pushIntradayReForecastData.exe*
xcopy /y dist\index_updateGjReForecastToSdl.exe index_updateGjReForecastToSdl.exe*
xcopy /y dist\index_pushChattSolarForecastData.exe index_pushChattSolarForecastData.exe*
xcopy /y dist\index_updateGujSolarForeWithStateGenFile.exe index_updateGujSolarForeWithStateGenFile.exe*