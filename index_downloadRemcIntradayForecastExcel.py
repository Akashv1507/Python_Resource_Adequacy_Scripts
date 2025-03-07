
import datetime as dt
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import logging
from src.appConfig import loadAppConfig
from src.remcWebScrapper import RemcWebScrapper


# Create and configure logger
logging.basicConfig(filename="files/logs/remcWebScrapping.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


appConfig = loadAppConfig()
reForecastFolderPath = appConfig['reForecastFolderPath']
username_str = appConfig['remcUsername']
password_str = appConfig['remcPwd']
remcUrl = appConfig['remcUrl']
currentDate=dt.datetime.now().date()
currentDateStr=currentDate.strftime('%Y-%m-%d')
logger.info(f"---------Intraday Solar wind forecast file download started for {currentDateStr}--------------")

d = DesiredCapabilities.EDGE
d['ms:loggingPrefs'] = { 'browser':'ALL' }
options = webdriver.EdgeOptions()
options.add_argument("--guest")
driver = webdriver.Edge(options=options)
wait = WebDriverWait(driver, 20)

# creating instance of RemcWebScrapper
obj_remcWebScrapper=RemcWebScrapper(driver)

try:

    driver.get(f'{remcUrl}')# Replace with your URL
    username = wait.until(EC.presence_of_element_located((By.ID, 'username')))
    username.clear()
    username.send_keys(username_str)

    password = wait.until(EC.presence_of_element_located((By.ID, 'passowrd')))  # Ensure you've the correct ID for the password
    password.clear()
    password.send_keys(password_str)

    other_button = wait.until(EC.element_to_be_clickable((By.ID, 'other')))
    other_button.click()

    question = wait.until(EC.presence_of_element_located((By.ID, 'question')))
    ans_text = obj_remcWebScrapper.evaluate_expression(question.text)  # This assumes you have an evaluate_expression function defined elsewhere
    ans = wait.until(EC.presence_of_element_located((By.ID, 'ans')))
    ans.send_keys(ans_text)

    login_button = wait.until(EC.element_to_be_clickable((By.ID, 'btn_login')))
    login_button.click()
    
    # # downloading mp Intraday solar and wind forecast excel
    # mpIntradaySolarDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/mpsldc_combined', 'tab_solar_state', 4)
    # mpIntradaySolarDf.to_csv(f"{reForecastFolderPath}Madhya_Pradesh_Solar_intraday_{currentDateStr}.csv",index=False)
    # logger.info(f"Madhya_Pradesh_Solar_intraday_{currentDateStr}.csv saved successfully")

    # mpIntradayWindDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/mpsldc_combined', 'tab_wind_state', 4)
    # mpIntradayWindDf.to_csv(f"{reForecastFolderPath}Madhya_Pradesh_Wind_intraday_{currentDateStr}.csv",index=False)
    # logger.info(f"Madhya_Pradesh_Wind_intraday_{currentDateStr}.csv saved successfully")

    # # downloading MH Intraday solar and wind forecast excel
    # mhIntradaySolarDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/msldc_combined', 'tab_solar_state', 4)
    # mhIntradaySolarDf.to_csv(f"{reForecastFolderPath}Maharashtra_Solar_intraday_{currentDateStr}.csv",index=False)
    # logger.info(f"Maharashtra_Solar_intraday_{currentDateStr}.csv saved successfully")

    # mhIntradayWindDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/msldc_combined', 'tab_wind_state', 4)
    # mhIntradayWindDf.to_csv(f"{reForecastFolderPath}Maharashtra_Wind_intraday_{currentDateStr}.csv",index=False)
    # logger.info(f"Maharashtra_Wind_intraday_{currentDateStr}.csv saved successfully")

    # downloading GJ Intraday solar and wind forecast excel
    gjIntradaySolarDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/gj_combined', 'tab_solar_state',4)
    gjIntradaySolarDf.to_csv(f"{reForecastFolderPath}Gujarat_Solar_intraday_{currentDateStr}.csv",index=False)
    logger.info(f"Gujarat_Solar_intraday_{currentDateStr}.csv saved successfully")

    gjIntradayWindDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/gj_combined', 'tab_wind_state',4)
    gjIntradayWindDf.to_csv(f"{reForecastFolderPath}Gujarat_Wind_intraday_{currentDateStr}.csv",index=False)
    logger.info(f"Gujarat_Wind_intraday_{currentDateStr}.csv saved successfully")
    driver.quit()
except:
    logger.error(f"coudnt load REMC website. Download data manually")
    driver.quit()