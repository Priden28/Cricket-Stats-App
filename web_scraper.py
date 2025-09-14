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
                logger.info(f"Attempting to click Next button: {next_button.tag_name} with text '{next_button.text}' and href '{next_button.get_attribute('href')}'")
                
                # Scroll to the button to ensure it's visible
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(1)  # Brief pause after scrolling
                
                # Get current URL and page content to compare after click
                current_url = self.driver.current_url
                logger.info(f"Current URL before click: {current_url}")
                
                # Get a unique identifier from current page
                try:
                    current_page_identifier = self.driver.find_element(By.CSS_SELECTOR, "table tr:nth-child(2)").text[:100]
                    logger.info(f"Current page identifier (first 100 chars): {current_page_identifier}")
                except Exception as e:
                    current_page_identifier = ""
                    logger.warning(f"Could not get page identifier: {e}")
                
                # Try clicking the button using JavaScript as backup
                try:
                    # First try normal click
                    next_button.click()
                    logger.info("Normal click executed")
                except Exception as e:
                    logger.warning(f"Normal click failed: {e}, trying JavaScript click")
                    # Fallback to JavaScript click
                    self.driver.execute_script("arguments[0].click();", next_button)
                    logger.info("JavaScript click executed")
                
                # Wait longer for the page to change
                logger.info("Waiting for page to change...")
                time.sleep(3)  # Initial wait
                
                # Check if the page actually changed by comparing content
                max_attempts = 15  # 15 attempts, 2 seconds each = 30 seconds max
                for attempt in range(max_attempts):
                    try:
                        time.sleep(2)
                        new_url = self.driver.current_url
                        
                        # Check URL change first (most reliable)
                        if new_url != current_url:
                            logger.info(f"URL changed from {current_url} to {new_url} (attempt {attempt + 1})")
                            return True
                        
                        # Check table content change
                        try:
                            new_page_identifier = self.driver.find_element(By.CSS_SELECTOR, "table tr:nth-child(2)").text[:100]
                            if new_page_identifier != current_page_identifier and new_page_identifier:
                                logger.info(f"Page content changed (attempt {attempt + 1})")
                                logger.info(f"Old content: {current_page_identifier}")
                                logger.info(f"New content: {new_page_identifier}")
                                return True
                        except Exception as e:
                            logger.debug(f"Could not check table content change: {e}")
                        
                        # Check page number indicator if available
                        try:
                            page_info_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Page') and contains(text(), 'of')]")
                            if page_info_elements:
                                new_page_info = page_info_elements[0].text
                                logger.debug(f"Page info: {new_page_info}")
                        except Exception:
                            pass
                        
                        logger.debug(f"Page hasn't changed yet (attempt {attempt + 1}/{max_attempts})")
                        
                    except Exception as e:
                        logger.debug(f"Error during page change check (attempt {attempt + 1}): {e}")
                        continue
                
                logger.warning("Page content didn't change after clicking Next - likely on last page or navigation failed")
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
            max_pages = 100  # Increased safety limit
            consecutive_empty_pages = 0  # Track empty pages to break early if needed
            
            while page_number <= max_pages:
                logger.info(f"Scraping page {page_number}")
                
                # Add a small delay to ensure page is fully rendered
                time.sleep(3)
                
                # Scrape current page data
                page_data = self.scrape_current_page_data()
                
                if page_data:
                    all_data.extend(page_data)
                    consecutive_empty_pages = 0  # Reset counter
                    logger.info(f"Page {page_number}: scraped {len(page_data)} rows, total so far: {len(all_data)}")
                else:
                    consecutive_empty_pages += 1
                    logger.warning(f"No data found on page {page_number} (empty page #{consecutive_empty_pages})")
                    
                    # If we get 3 consecutive empty pages, something is wrong
                    if consecutive_empty_pages >= 3:
                        logger.error("Too many consecutive empty pages - stopping scraping")
                        break
                
                # Try to go to next page
                logger.info(f"Attempting to navigate from page {page_number} to page {page_number + 1}")
                if self.click_next_button():
                    page_number += 1
                    logger.info(f"Successfully moved to page {page_number}")
                else:
                    logger.info(f"No more pages available. Finished scraping at page {page_number}")
                    logger.info(f"Final totals - Pages: {page_number}, Total rows: {len(all_data)}")
                    break
            
            if page_number > max_pages:
                logger.warning(f"Reached maximum page limit ({max_pages})")
            
            # Final summary
            logger.info(f"Scraping complete:")
            logger.info(f"  - Total pages processed: {page_number}")
            logger.info(f"  - Total rows collected: {len(all_data)}")
            logger.info(f"  - Average rows per page: {len(all_data) / page_number if page_number > 0 else 0:.1f}")
            
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