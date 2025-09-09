# cricket_service.py
import logging
import threading
import pandas as pd
from database import DatabaseManager
from web_scraper import WebScraper
from data_processor import DataProcessor
from config import DATASET_CONFIGS
from datetime import datetime

logger = logging.getLogger(__name__)

class CricketService:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.data_processor = DataProcessor(self.db_manager)
        self.scraper = WebScraper()
        self.lock = threading.Lock()
    
    def scrape_and_process_data(self, dataset_type):
        """Main method to scrape and process data for a given dataset type"""
        if dataset_type not in DATASET_CONFIGS:
            raise ValueError(f"Invalid dataset type: {dataset_type}")
        
        config = DATASET_CONFIGS[dataset_type]
        
        try:
            with self.lock:
                logger.info(f"Starting scraping for {dataset_type}")
                
                # Get latest date from database
                latest_date = self.db_manager.fetch_latest_date(dataset_type)
                latest_date_str = (
                    WebScraper.format_date_for_url(latest_date.strftime("%Y-%m-%d"))
                    if latest_date else '13+Aug+2022'
                )

                scraped_data = self.scraper.scrape_dataset(dataset_type, latest_date_str)
                
                if not scraped_data:
                    logger.warning(f"No new data found for {dataset_type}")
                    return pd.DataFrame()

                # Process and save data to the database
                if dataset_type == 'team':
                    processed_df = self.data_processor.process_team_data(
                        scraped_data, config['columns']
                    )
                elif dataset_type == 'batting':
                    processed_df = self.data_processor.process_batting_data(
                        scraped_data, config['columns']
                    )
                elif dataset_type == 'bowling':
                    processed_df = self.data_processor.process_bowling_data(
                        scraped_data, config['columns']
                    )
                
                logger.info(f"Successfully processed and inserted {len(processed_df)} new rows for {dataset_type}")
                return processed_df
                
        except Exception as e:
            logger.error(f"Error processing {dataset_type} data: {e}")
            raise
        finally:
            self.scraper.close()
    
    def close(self):
        """Clean up resources"""
        self.scraper.close()
        self.db_manager.close()
        logger.info("CricketService resources closed")