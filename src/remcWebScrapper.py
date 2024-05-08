from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from io import StringIO
import pandas as pd
import logging
# Create and configure logger
logging.basicConfig(filename="files/logs/remcWebScrapping.log", format='%(asctime)s %(name)s %(levelname)s:%(message)s', filemode='a')
 # Creating an object
logger = logging.getLogger(__name__)
# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


class RemcWebScrapper():

    def __init__(self,driver) -> None:

        self.driver= driver

    def wait_for_elements(self, by_method, identifier):
        return WebDriverWait(self.driver, 20).until(
            EC.presence_of_all_elements_located((by_method, identifier))
        )
    # Define a function to wait for an element to be clickable
    def wait_for_element(self, by_method, identifier):
        return WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((by_method, identifier))
        )
    def evaluate_expression(self, expression):
        # Removing the '=' sign and any spaces
        expression = expression.replace('=', '').replace(' ', '')

        # Using 'eval' cautiously to evaluate the simple mathematical expression
        try:
            result = eval(expression)
            return result
        except Exception as e:
            return f"An error occurred: {e}"

    def find_data_from_url(self, url, solarWindFlag, dropdownNo):
        df=None
        try:
            self.driver.get(url)
            tab=self.wait_for_element(By.ID, solarWindFlag)
            # Scroll the tab element into view
            self.driver.execute_script("arguments[0].scrollIntoView();", tab)
            # Click the tab element using JavaScript
            self.driver.execute_script("arguments[0].click();", tab)

            dropdowns = self.wait_for_elements(By.CSS_SELECTOR, 'a.collapse-link')
            dropdowns[dropdownNo].click()

            # dropdowns[4].click() for intraday
            # dropdowns[1].click() for day ahead
            dropdown = Select(self.wait_for_element(By.NAME,'id1_length'))
            # Select the '144' option by the value of the option
            dropdown.select_by_value('144')

            # Find the table element you're interested in
            table = self.wait_for_element(By.ID, 'id1')
            # Extract the HTML of the table
            html = table.get_attribute('outerHTML')
            # Use StringIO to create a file-like object from the HTML string
            html_file = StringIO(html)
            # Here, we're directly reading the HTML of the table into a pandas DataFrame.
            # Pandas will automatically find and read the tabular data in the HTML.
            df = pd.read_html(html_file)[0]
            return df
        except Exception as err:
            logger.error(f'error while scrapping url->{url}')
            logger.error(f'and solarWindFlag was {solarWindFlag}')
            return df

    # def find_data_from_url_da(self, url, solarWindFlag):
    #     df=None
    #     try:
    #         self.driver.get(url)
    #         tab=self.wait_for_element(By.ID, solarWindFlag)
    #         tab.click()
    #         dropdowns = self.wait_for_elements(By.CSS_SELECTOR, 'a.collapse-link')
    #         dropdowns[1].click()
    #         dropdown = Select(self.wait_for_element(By.NAME,'id1_length'))
    #         # Select the '144' option by the value of the option
    #         dropdown.select_by_value('144')

    #         # Find the table element you're interested in
    #         table = self.driver.find_element(By.ID,'id1')
    #         # Extract the HTML of the table
    #         html = table.get_attribute('outerHTML')
    #         # Use StringIO to create a file-like object from the HTML string
    #         html_file = StringIO(html)
    #         # Here, we're directly reading the HTML of the table into a pandas DataFrame.
    #         # Pandas will automatically find and read the tabular data in the HTML.
    #         df = pd.read_html(html_file)[0]
    #         return df
    #     except Exception as err:
    #         print(err)
    #         return df