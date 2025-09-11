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
        
        try:
            self.driver = webdriver.Chrome(options=options)
            # Set page load timeout
            self.driver.set_page_load_timeout(30)
            # Set implicit wait
            self.driver.implicitly_wait(10)
            logger.info("WebDriver initialized successfully")
            return self.driver
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            raise
    
    def scrape_current_page_data(self):
        """Scrape table data from the current page"""
        try:
            wait = WebDriverWait(self.driver, 15)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
            logger.info("Table found on current page")
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
            # Wait a bit for the page to fully load
            time.sleep(1)
            
            # Based on the screenshot, look for the "Next" link specifically
            # Try multiple strategies to find the Next button
            
            # Strategy 1: Look for link with text "Next"
            try:
                next_link = self.driver.find_element(By.LINK_TEXT, "Next")
                if next_link.is_enabled() and next_link.is_displayed():
                    logger.info("Found Next button using LINK_TEXT")
                    return next_link
            except NoSuchElementException:
                pass
            
            # Strategy 2: Look for link with partial text "Next"
            try:
                next_link = self.driver.find_element(By.PARTIAL_LINK_TEXT, "Next")
                if next_link.is_enabled() and next_link.is_displayed():
                    logger.info("Found Next button using PARTIAL_LINK_TEXT")
                    return next_link
            except NoSuchElementException:
                pass
            
            # Strategy 3: Look for any clickable element containing "Next"
            try:
                elements = self.driver.find_elements(By.XPATH, "//*[contains(text(),'Next')]")
                for element in elements:
                    if element.is_enabled() and element.is_displayed() and element.tag_name in ['a', 'button', 'input']:
                        logger.info(f"Found Next button using XPath on {element.tag_name}")
                        return element
            except NoSuchElementException:
                pass
            
            # Strategy 4: Look for pagination links (the page shows navigation at bottom)
            try:
                # Look for links in pagination area
                pagination_links = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'page=') or contains(text(), 'Next')]")
                for link in pagination_links:
                    if link.is_enabled() and link.is_displayed() and 'next' in link.text.lower():
                        logger.info("Found Next button in pagination area")
                        return link
            except Exception:
                pass
                
            logger.info("No Next button found - likely on last page")
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
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(1)  # Brief pause after scrolling
                
                # Get current URL to compare after click
                current_url = self.driver.current_url
                
                # Try clicking the button
                next_button.click()
                logger.info("Clicked Next button")
                
                # Wait for the page to change
                time.sleep(3)
                
                # Check if URL changed or wait for new content
                try:
                    # Wait for either URL change or table to reload
                    WebDriverWait(self.driver, 15).until(
                        lambda driver: driver.current_url != current_url or
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
                    )
                    logger.info("Successfully navigated to next page")
                    return True
                except TimeoutException:
                    logger.warning("Page didn't change after clicking Next - might be on last page")
                    return False
                
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
            logger.info(f"Loading URL: {url}")
            self.driver.get(url)
            
            # Wait for initial page to load completely
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
            )
            logger.info(f"Initial page loaded successfully")
            
            all_data = []
            page_number = 1
            max_pages = 50  # Safety limit to prevent infinite loops
            
            while page_number <= max_pages:
                logger.info(f"Scraping page {page_number}")
                
                # Scrape current page data
                page_data = self.scrape_current_page_data()
                all_data.extend(page_data)
                
                logger.info(f"Page {page_number}: scraped {len(page_data)} rows, total so far: {len(all_data)}")
                
                # If no data on current page, might be an issue
                if not page_data:
                    logger.warning(f"No data found on page {page_number}")
                    break
                
                # Try to go to next page
                if self.click_next_button():
                    page_number += 1
                    # Small delay between pages
                    time.sleep(2)
                else:
                    logger.info(f"Finished scraping. Total pages: {page_number}, Total rows: {len(all_data)}")
                    break
            
            if page_number > max_pages:
                logger.warning(f"Reached maximum page limit ({max_pages})")
            
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
            try:
                self.driver.quit()
                logger.info("WebDriver closed successfully")
            except Exception as e:
                logger.error(f"Error closing WebDriver: {e}")
            finally:
                self.driver = None