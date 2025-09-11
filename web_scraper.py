import uuid
import os
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from config import CHROME_OPTIONS, CHROME_BINARY_LOCATION, URL_TEMPLATES, BASE_URL

logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self):
        self.driver = None
    
    def initialize_driver(self):
        """Initialize Chrome WebDriver with optimal settings"""
        session_id = str(uuid.uuid4())
        user_data_dir = f"/tmp/chrome-user-data-{session_id}"
        data_path = f"/tmp/chrome-data-{session_id}"
        cache_dir = f"/tmp/chrome-cache-{session_id}"
        
        # Create directories
        for directory in [user_data_dir, data_path, cache_dir]:
            os.makedirs(directory, exist_ok=True)
        
        options = webdriver.ChromeOptions()
        
        # Add all Chrome options
        for option in CHROME_OPTIONS:
            options.add_argument(option)
        
        # Add session-specific options
        options.add_argument(f'--user-data-dir={user_data_dir}')
        options.add_argument(f'--data-path={data_path}')
        options.add_argument(f'--disk-cache-dir={cache_dir}')
        options.binary_location = CHROME_BINARY_LOCATION
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        self.driver = webdriver.Chrome(options=options)
        return self.driver
    
    def scrape_page_data(self, url):
        """Scrape table data from the given URL"""
        if not self.driver:
            self.initialize_driver()
        
        try:
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 10)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
        except Exception as e:
            logger.error(f"Error locating table: {e}")
            return []

        html_content = self.driver.page_source
        soup = BeautifulSoup(html_content, "html.parser")
        rows = soup.select("table tr")

        data = []
        for row in rows[1:]:  # Skip the header row
            row_data = [cell.get_text(strip=True) for cell in row.select("td")]
            if row_data and len(row_data) > 1 and row_data[0] != 'Page1of2018':
                data.append(row_data)
        
        logger.info(f"Scraped {len(data)} rows from {url}")
        return data
    
    def generate_url(self, table_type, start_date):
        """Generate URL for the specific dataset type and date"""
        url_template = URL_TEMPLATES.get(table_type)
        if not url_template:
            raise ValueError(f"Unknown table type: {table_type}")
        
        url = url_template.format(start_date=start_date)
        return BASE_URL + url
    
    @staticmethod
    def format_date_for_url(date_str):
        """Format date string for URL"""
        date = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
        return date.strftime("%d+%b+%Y")
    
    def scrape_dataset(self, dataset_type, start_date):
        """Scrape data for a specific dataset type from the given start date"""
        url = self.generate_url(dataset_type, start_date)
        logger.info(f"Scraping {dataset_type} data from: {url}")
        
        try:
            data = self.scrape_page_data(url)
            return data
        except Exception as e:
            logger.error(f"Failed to scrape {dataset_type} data: {e}")
            raise
    
    def close(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("WebDriver closed")