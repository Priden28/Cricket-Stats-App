import uuid
import os
import logging
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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
    
    def scrape_current_page_data(self):
        """Scrape table data from the current page"""
        try:
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
        
        logger.info(f"Scraped {len(data)} rows from current page")
        return data
    
    def has_next_button(self):
        """Check if there's a Next button available and clickable"""
        try:
            # Look for the Next button - it could be a link or button
            next_selectors = [
                "a[title='Next']",  # Link with title="Next"
                "a:contains('Next')",  # Link containing "Next" text
                "input[value='Next']",  # Input button with value="Next"
                "button:contains('Next')",  # Button containing "Next"
                ".paging a:contains('Next')",  # Next link in paging section
                "a[href*='page=']:contains('Next')"  # Link with page parameter containing "Next"
            ]
            
            for selector in next_selectors:
                try:
                    if selector.startswith("a:contains") or selector.startswith("button:contains") or "contains" in selector:
                        # For contains selectors, we need to find by text
                        elements = self.driver.find_elements(By.TAG_NAME, "a")
                        for element in elements:
                            if "next" in element.text.lower() and element.is_enabled():
                                return element
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if element.is_enabled():
                            return element
                except NoSuchElementException:
                    continue
            
            # Try finding by partial link text as fallback
            try:
                element = self.driver.find_element(By.PARTIAL_LINK_TEXT, "Next")
                if element.is_enabled():
                    return element
            except NoSuchElementException:
                pass
                
            return None
            
        except Exception as e:
            logger.error(f"Error checking for next button: {e}")
            return None
    
    def click_next_button(self):
        """Click the Next button if available"""
        next_button = self.has_next_button()
        if next_button:
            try:
                # Scroll to the button to ensure it's visible
                self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(1)  # Brief pause after scrolling
                
                # Try clicking the button
                next_button.click()
                
                # Wait for the page to load
                time.sleep(2)
                
                # Wait for the table to be present on the new page
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
                )
                
                logger.info("Successfully clicked Next button and loaded new page")
                return True
                
            except Exception as e:
                logger.error(f"Error clicking Next button: {e}")
                return False
        else:
            logger.info("No Next button found - reached last page")
            return False
    
    def scrape_page_data(self, url):
        """Scrape table data from all pages starting from the given URL"""
        if not self.driver:
            self.initialize_driver()
        
        try:
            self.driver.get(url)
            logger.info(f"Loaded initial URL: {url}")
            
            all_data = []
            page_number = 1
            
            while True:
                logger.info(f"Scraping page {page_number}")
                
                # Scrape current page data
                page_data = self.scrape_current_page_data()
                all_data.extend(page_data)
                
                logger.info(f"Page {page_number}: scraped {len(page_data)} rows, total so far: {len(all_data)}")
                
                # Try to go to next page
                if self.click_next_button():
                    page_number += 1
                else:
                    logger.info(f"Finished scraping. Total pages: {page_number}, Total rows: {len(all_data)}")
                    break
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error scraping pages: {e}")
            return []
    
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
            logger.info(f"Total rows scraped for {dataset_type}: {len(data)}")
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