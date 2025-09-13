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
        """Scrape table data from the current page with enhanced debugging"""
        try:
            wait = WebDriverWait(self.driver, 15)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
            logger.info("Table found on current page")
        except Exception as e:
            logger.error(f"Error locating table: {e}")
            return []

        # Wait a bit longer for dynamic content to load
        time.sleep(2)
        
        html_content = self.driver.page_source
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Find all tables and use the main data table
        tables = soup.find_all("table")
        logger.info(f"Found {len(tables)} tables on page")
        
        if not tables:
            logger.error("No tables found in page source")
            return []
        
        # Use the first table (or find the correct one if there are multiple)
        table = tables[0]
        rows = table.find_all("tr")
        
        logger.info(f"Found {len(rows)} total rows in table")
        
        data = []
        filtered_out_count = 0
        
        for i, row in enumerate(rows):
            # Skip header row (first row)
            if i == 0:
                header_cells = [cell.get_text(strip=True) for cell in row.find_all(['th', 'td'])]
                logger.info(f"Header row: {header_cells}")
                continue
                
            # Get all cells in the row
            cells = row.find_all('td')
            if not cells:
                logger.debug(f"Row {i}: No td cells found, skipping")
                continue
                
            row_data = [cell.get_text(strip=True) for cell in cells]
            
            # Debug: Log first few and last few rows
            if i <= 3 or i >= len(rows) - 3:
                logger.info(f"Row {i}: {row_data}")
            
            # Original filtering conditions with logging
            if not row_data:
                logger.debug(f"Row {i}: Empty row_data, skipping")
                filtered_out_count += 1
                continue
                
            if len(row_data) <= 1:
                logger.debug(f"Row {i}: Too few columns ({len(row_data)}), skipping")
                filtered_out_count += 1
                continue
                
            # Check the problematic filter condition
            if row_data[0] == 'Page1of2018':
                logger.debug(f"Row {i}: Filtered out pagination text: {row_data[0]}")
                filtered_out_count += 1
                continue
                
            # Additional checks for common pagination/navigation text
            first_cell = row_data[0].lower()
            if any(pagination_text in first_cell for pagination_text in ['page', 'next', 'previous', 'first', 'last']):
                logger.debug(f"Row {i}: Filtered out navigation text: {row_data[0]}")
                filtered_out_count += 1
                continue
                
            # If we get here, it's a valid data row
            data.append(row_data)
        
        logger.info(f"Page scraping summary:")
        logger.info(f"  - Total rows in table: {len(rows)}")
        logger.info(f"  - Valid data rows extracted: {len(data)}")
        logger.info(f"  - Rows filtered out: {filtered_out_count}")
        
        # If we got very few rows, log the raw table content for debugging
        if len(data) < 10:
            logger.warning(f"Very few rows extracted ({len(data)}), logging raw table content:")
            for i, row in enumerate(rows[:5]):  # First 5 rows
                cells = row.find_all(['td', 'th'])
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                logger.warning(f"Raw row {i}: {cell_texts}")
        
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
                
                # Get current URL and page content hash to compare after click
                current_url = self.driver.current_url
                # Get a unique identifier from current page (like first few rows of data)
                try:
                    current_page_identifier = self.driver.find_element(By.CSS_SELECTOR, "table tr:nth-child(2)").text
                except:
                    current_page_identifier = ""
                
                # Try clicking the button
                next_button.click()
                logger.info("Clicked Next button")
                
                # Wait longer for the page to change
                time.sleep(5)
                
                # Check if the page actually changed by comparing content
                try:
                    # Wait up to 20 seconds for page content to change
                    for attempt in range(10):  # 10 attempts, 2 seconds each = 20 seconds max
                        time.sleep(2)
                        try:
                            new_page_identifier = self.driver.find_element(By.CSS_SELECTOR, "table tr:nth-child(2)").text
                            new_url = self.driver.current_url
                            
                            # Check if either URL changed OR table content changed
                            if new_url != current_url or new_page_identifier != current_page_identifier:
                                logger.info(f"Successfully navigated to next page (attempt {attempt + 1})")
                                return True
                        except:
                            # If we can't find the table row, page might still be loading
                            continue
                    
                    logger.warning("Page content didn't change after clicking Next - likely on last page")
                    return False
                    
                except Exception as e:
                    logger.error(f"Error waiting for page change: {e}")
                    return False
                    
            except Exception as e:
                logger.error(f"Error clicking Next button: {e}")
                return False
        else:
            logger.info("No Next button found - reached last page")
            return False
    
    def scrape_page_data(self, url):
        """Scrape table data from all pages starting from the given URL with enhanced debugging"""
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
            max_pages = 100
            
            # Track data across pages for debugging
            page_data_counts = []
            
            while page_number <= max_pages:
                logger.info(f"=== SCRAPING PAGE {page_number} ===")
                
                # Add a delay to ensure page is fully rendered
                time.sleep(3)
                
                # Scrape current page data
                page_data = self.scrape_current_page_data()
                page_data_counts.append(len(page_data))
                
                if page_data:
                    all_data.extend(page_data)
                    logger.info(f"Page {page_number}: Added {len(page_data)} rows, total so far: {len(all_data)}")
                    
                    # Log sample data from this page
                    if len(page_data) > 0:
                        logger.info(f"Sample from page {page_number}: {page_data[0][:3] if len(page_data[0]) > 3 else page_data[0]}")
                        
                else:
                    logger.warning(f"Page {page_number}: No data extracted!")
                
                # Check for next page
                logger.info(f"Checking for next page after page {page_number}")
                next_button = self.has_next_button()
                
                if next_button:
                    logger.info(f"Next button found, attempting to navigate to page {page_number + 1}")
                    if self.click_next_button():
                        page_number += 1
                        logger.info(f"Successfully navigated to page {page_number}")
                    else:
                        logger.warning(f"Failed to navigate to next page, stopping at page {page_number}")
                        break
                else:
                    logger.info(f"No next button found, finishing at page {page_number}")
                    break
            
            # Final comprehensive summary
            logger.info(f"=== SCRAPING COMPLETE ===")
            logger.info(f"Total pages processed: {page_number}")
            logger.info(f"Total rows collected: {len(all_data)}")
            logger.info(f"Data per page: {page_data_counts}")
            logger.info(f"Average rows per page: {sum(page_data_counts) / len(page_data_counts) if page_data_counts else 0:.1f}")
            
            # If we got much fewer rows than expected, log a warning
            if len(all_data) < 100 and page_number > 2:
                logger.warning(f"WARNING: Only collected {len(all_data)} rows across {page_number} pages. This seems low.")
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error scraping pages: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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