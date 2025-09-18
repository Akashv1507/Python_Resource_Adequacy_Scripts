
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
tommDate=dt.datetime.now().date() + dt.timedelta(days=1)
tommDateStr=tommDate.strftime('%Y-%m-%d')

logger.info(f"---------Day Ahead Solar wind forecast file download started for {tommDateStr}--------------") 

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
    
    # downloading mp DA solar and wind forecast excel
    mpDaSolarDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/da_wr/MPTCL', 'DA_SOLAR',1)
    mpDaSolarDf.to_csv(f"{reForecastFolderPath}Madhya_Pradesh_Solar_da_{tommDateStr}.csv",index=False)
    logger.info(f"Madhya_Pradesh_Solar_da_{tommDateStr}.csv saved successfully")
    
    mpDaWindDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/da_wr/MPTCL', 'DA_WIND',1)
    mpDaWindDf.to_csv(f"{reForecastFolderPath}Madhya_Pradesh_Wind_da_{tommDateStr}.csv",index=False)
    logger.info(f"Madhya_Pradesh_Wind_da_{tommDateStr}.csv saved successfully")
    
    # downloading MH DA solar and wind forecast excel
    mhDaSolarDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/da_wr/MSLDC', 'DA_SOLAR',1)
    mhDaSolarDf.to_csv(f"{reForecastFolderPath}Maharashtra_Solar_da_{tommDateStr}.csv",index=False)
    logger.info(f"Maharashtra_Solar_da_{tommDateStr}.csv saved successfully")

    mhDaWindDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/da_wr/MSLDC', 'DA_WIND',1)
    mhDaWindDf.to_csv(f"{reForecastFolderPath}Maharashtra_Wind_da_{tommDateStr}.csv",index=False)
    logger.info(f"Maharashtra_Wind_da_{tommDateStr}.csv saved successfully")

    # downloading GJ DA solar and wind forecast excel
    gjDaSolarDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/da_wr/GUJARAT', 'DA_SOLAR',1)
    gjDaSolarDf.to_csv(f"{reForecastFolderPath}Gujarat_Solar_da_{tommDateStr}.csv",index=False)
    logger.info(f"Gujarat_Solar_da_{tommDateStr}.csv saved successfully")

    gjDaWindDf=obj_remcWebScrapper.find_data_from_url(f'{remcUrl}index.php/ext_report/da_wr/GUJARAT', 'DA_WIND',1)
    gjDaWindDf.to_csv(f"{reForecastFolderPath}Gujarat_Wind_da_{tommDateStr}.csv",index=False)
    logger.info(f"Gujarat_Wind_da_{tommDateStr}.csv saved successfully")
    driver.quit()
    
except:
    logger.error(f"coudnt load REMC website. Download data manually")
    driver.quit()