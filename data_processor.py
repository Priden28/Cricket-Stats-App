# data_processor.py
import re
import numpy as np
import pandas as pd
import logging
from config import TEAM_MAPPING
from datetime import datetime

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def clean_data(self, data, min_columns):
        """Clean raw scraped data and apply team mapping"""
        cleaned_data = []
        for row in data:
            if all(isinstance(cell, str) for cell in row):
                if len(row) >= min_columns:
                    team_code = row[0]
                    team_name = TEAM_MAPPING.get(team_code, team_code)
                    row[0] = team_name
                    cleaned_data.append(row)
        
        logger.info(f"Cleaned {len(cleaned_data)} rows")
        return np.array(cleaned_data)
    
    def convert_to_dataframe(self, cleaned_data_array, columns):
        """Convert cleaned data array to pandas DataFrame and drop empty columns"""
        cleaned_data_array = [row[:len(columns)] for row in cleaned_data_array]
        df = pd.DataFrame(cleaned_data_array, columns=columns)
        
        # Drop columns with empty names or that are completely empty
        columns_to_drop = []
        for col in df.columns:
            if col == "" or col.strip() == "":
                columns_to_drop.append(col)
                logger.info(f"Dropping empty column: '{col}'")
        
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            logger.info(f"Dropped {len(columns_to_drop)} empty columns")
        
        return df
    
    def clean_opposition_column(self, dataframe):
        """Clean the Opposition column by removing 'v' prefix"""
        if 'Opposition' in dataframe.columns:
            dataframe['Opposition'] = dataframe['Opposition'].str.replace('^v', '', regex=True)
        return dataframe
    
    def split_player_and_team(self, dataframe, team_mapping):
        """Split Player column and extract team from within parentheses"""
        def extract_team(player_name):
            match = re.search(r'\((.*?)\)', player_name)
            if match:
                team_code = match.group(1)
                return team_mapping.get(team_code, None)
            return None
        
        def remove_team_from_player(player_name):
            return re.sub(r'\(.*?\)', '', player_name).strip()
        
        # Create the new "Team" column by extracting the team from the Player column
        dataframe['Team'] = dataframe['Player'].apply(extract_team)
        # Clean the "Player" column by removing the team code
        dataframe['Player'] = dataframe['Player'].apply(remove_team_from_player)
        
        return dataframe
    
    def process_team_score(self, dataframe):
        """Process the ScoreDescending column for team data"""
        def extract_score_and_wickets(score):
            declared = 0
            wickets = 10
            
            if 'd' in score:
                declared = 1
                score = score.replace('d', '')
            
            if '/' in score:
                score, wickets = score.split('/')
                score = int(score)
                wickets = int(wickets)
            else:
                score = int(score)
            
            return score, wickets, declared
        
        dataframe[['ScoreDescending', 'Wickets', 'Declared']] = dataframe['ScoreDescending'].apply(
            lambda x: pd.Series(extract_score_and_wickets(x))
        )
        return dataframe
    
    def process_overs_column(self, dataframe, column_name='Overs'):
        """Convert overs format (5.2 means 5 overs and 2 balls) to float"""
        def convert_overs_to_float(overs):
            try:
                if isinstance(overs, str):
                    overs = overs.strip()
                    if '.' in overs:
                        overs_parts = overs.split('.')
                        overs_in_float = int(overs_parts[0]) + (int(overs_parts[1]) / 6)
                        return overs_in_float
                    else:
                        return float(overs)
                return float(overs)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert overs value: {overs}")
                return 0.0

        dataframe[column_name] = dataframe[column_name].apply(convert_overs_to_float)
        return dataframe
    
    def process_team_data(self, scraped_data, columns):
        """Process and store team data in the database"""
        logger.info(f"Starting to process team data with {len(scraped_data)} scraped rows")
        
        if not scraped_data:
            logger.warning("No scraped data provided for team processing")
            return pd.DataFrame()
        
        try:
            cleaned_data = self.clean_data(scraped_data, len(columns))
            if len(cleaned_data) == 0:
                logger.warning("No data left after cleaning")
                return pd.DataFrame()
                
            df = self.convert_to_dataframe(cleaned_data, columns)
            df = self.clean_opposition_column(df)
            df = self.process_team_score(df)
            df = self.process_overs_column(df, column_name='Overs')
            
            # Convert data types with error handling
            try:
                df['RPO'] = pd.to_numeric(df['RPO'], errors='coerce').fillna(0.0)
                df['Inns'] = pd.to_numeric(df['Inns'], errors='coerce').fillna(1).astype(int)
                df['Lead'] = pd.to_numeric(df['Lead'], errors='coerce').fillna(0).astype(int)
                df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
            except Exception as e:
                logger.error(f"Error converting data types: {e}")
                return df
            
            # Log the final DataFrame structure
            logger.info(f"Final DataFrame columns: {list(df.columns)}")
            logger.info(f"DataFrame shape: {df.shape}")
            
            # Insert data into database
            inserted_count = self._insert_team_data_to_db(df)
            
            logger.info(f"Team data processing complete: {inserted_count} rows inserted")
            return df
            
        except Exception as e:
            logger.error(f"Error processing team data: {e}")
            return pd.DataFrame()
    
    def _insert_team_data_to_db(self, df):
        """Insert team data into database with improved error handling"""
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        for index, row in df.iterrows():
            try:
                # Check if row exists
                if not self.db_manager.row_exists_team(
                    row['Team'], row['ScoreDescending'], row['Ground'], row['Start Date']
                ):
                    # Prepare data tuple
                    row_data = (
                        row['Team'],
                        row['ScoreDescending'],
                        row['Overs'],
                        row['RPO'],
                        row['Lead'],
                        row['Inns'],
                        row['Result'],
                        row['Opposition'],
                        row['Ground'],
                        row['Start Date'],
                        row['Declared'],
                        row['Wickets']
                    )
                    
                    # Insert using database manager method
                    self.db_manager.insert_team_record(row_data)
                    inserted_count += 1
                    
                    if inserted_count % 10 == 0:
                        logger.info(f"Inserted {inserted_count} team records so far...")
                else:
                    duplicate_count += 1
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error inserting team row {index}: {e}")
                continue
        
        logger.info(f"Team insertion summary - Inserted: {inserted_count}, Duplicates: {duplicate_count}, Errors: {error_count}")
        return inserted_count

    def process_batting_data(self, scraped_data, columns):
        """Process and store batting data in the database"""
        logger.info(f"Starting to process batting data with {len(scraped_data)} scraped rows")
        
        if not scraped_data:
            logger.warning("No scraped data provided for batting processing")
            return pd.DataFrame()
        
        try:
            cleaned_data = self.clean_data(scraped_data, len(columns))
            if len(cleaned_data) == 0:
                logger.warning("No data left after cleaning")
                return pd.DataFrame()
                
            df = self.convert_to_dataframe(cleaned_data, columns)
            df = self.clean_opposition_column(df)
            df = self.split_player_and_team(df, TEAM_MAPPING)
            
            # Clean and convert data like in original code
            df = df[~df['RunsDescending'].isin(['DNB', 'absent', 'sub'])]
            df['Not Out'] = df['RunsDescending'].apply(lambda x: 1 if '*' in str(x) else 0)
            df['RunsDescending'] = df['RunsDescending'].str.replace('*', '', regex=False)
            
            # Remove Mins column if it exists
            if 'Mins' in df.columns:
                df = df.drop(columns=['Mins'])
            
            # Convert data types with error handling
            try:
                df['RunsDescending'] = pd.to_numeric(df['RunsDescending'], errors='coerce').fillna(0).astype(int)
                df['4s'] = pd.to_numeric(df['4s'], errors='coerce').fillna(0).astype(int)
                df['6s'] = pd.to_numeric(df['6s'], errors='coerce').fillna(0).astype(int)
                df['BF'] = pd.to_numeric(df['BF'], errors='coerce').fillna(0).astype(int)
                df['Inns'] = pd.to_numeric(df['Inns'], errors='coerce').fillna(1).astype(int)
                df['SR'] = pd.to_numeric(df['SR'].str.strip().replace('-', '0'), errors='coerce').fillna(0.0)
                df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
            except Exception as e:
                logger.error(f"Error converting batting data types: {e}")
                return df
            
            # Log the final DataFrame structure
            logger.info(f"Final batting DataFrame columns: {list(df.columns)}")
            logger.info(f"Batting DataFrame shape: {df.shape}")
            
            # Insert data into database
            inserted_count = self._insert_batting_data_to_db(df)
            
            logger.info(f"Batting data processing complete: {inserted_count} rows inserted")
            return df
            
        except Exception as e:
            logger.error(f"Error processing batting data: {e}")
            return pd.DataFrame()
    
    def _insert_batting_data_to_db(self, df):
        """Insert batting data into database with improved error handling"""
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        for index, row in df.iterrows():
            try:
                # Check if row exists
                if not self.db_manager.row_exists_batting(
                    row['Player'], row['RunsDescending'], row['Ground'], row['Start Date']
                ):
                    # Prepare data tuple
                    row_data = (
                        row['Player'],
                        row['RunsDescending'],
                        row['BF'],
                        row['4s'],
                        row['6s'],
                        row['SR'],
                        row['Inns'],
                        row['Opposition'],
                        row['Ground'],
                        row['Start Date'],
                        row['Not Out'],
                        row['Team']
                    )
                    
                    # Insert using database manager method
                    self.db_manager.insert_batting_record(row_data)
                    inserted_count += 1
                    
                    if inserted_count % 10 == 0:
                        logger.info(f"Inserted {inserted_count} batting records so far...")
                else:
                    duplicate_count += 1
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error inserting batting row {index}: {e}")
                continue
        
        logger.info(f"Batting insertion summary - Inserted: {inserted_count}, Duplicates: {duplicate_count}, Errors: {error_count}")
        return inserted_count

    def process_bowling_data(self, scraped_data, columns):
        """Process and store bowling data in the database"""
        logger.info(f"Starting to process bowling data with {len(scraped_data)} scraped rows")
        
        if not scraped_data:
            logger.warning("No scraped data provided for bowling processing")
            return pd.DataFrame()
        
        try:
            cleaned_data = self.clean_data(scraped_data, len(columns))
            if len(cleaned_data) == 0:
                logger.warning("No data left after cleaning")
                return pd.DataFrame()
                
            df = self.convert_to_dataframe(cleaned_data, columns)
            df = self.clean_opposition_column(df)
            df = self.split_player_and_team(df, TEAM_MAPPING)
            
            # Clean and convert data like in original code - FILTER OUT DNB/absent FIRST
            df = df[~df['WktsDescending'].isin(['DNB', 'absent', 'sub'])]
            # Also filter out DNB from Overs column before processing
            df = df[~df['Overs'].isin(['DNB', 'absent', 'sub'])]
            df = self.process_overs_column(df, column_name='Overs')
            
            # Convert data types with error handling
            try:
                # Handle Inns column more carefully
                df['Inns'] = df['Inns'].apply(lambda x: int(str(x).split()[0]) if str(x).split()[0].isdigit() else 1)
                
                df['Mdns'] = pd.to_numeric(df['Mdns'].replace('-', '0'), errors='coerce').fillna(0).astype(int)
                df['Runs'] = pd.to_numeric(df['Runs'].replace('-', '0'), errors='coerce').fillna(0).astype(int)
                df['WktsDescending'] = pd.to_numeric(df['WktsDescending'].replace('-', '0'), errors='coerce').fillna(0).astype(int)
                df['Econ'] = pd.to_numeric(df['Econ'].replace('-', '0'), errors='coerce').fillna(0.0)
                df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
            except Exception as e:
                logger.error(f"Error converting bowling data types: {e}")
                return df

            # Log the final DataFrame structure
            logger.info(f"Final bowling DataFrame columns: {list(df.columns)}")
            logger.info(f"Bowling DataFrame shape: {df.shape}")

            # Insert data into database
            inserted_count = self._insert_bowling_data_to_db(df)
            
            logger.info(f"Bowling data processing complete: {inserted_count} rows inserted")
            return df
            
        except Exception as e:
            logger.error(f"Error processing bowling data: {e}")
            return pd.DataFrame()
    
    def _insert_bowling_data_to_db(self, df):
        """Insert bowling data into database with improved error handling"""
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        for index, row in df.iterrows():
            try:
                # Check if row exists
                if not self.db_manager.row_exists_bowling(
                    row['Player'], row['Overs'], row['Mdns'], row['Runs'], 
                    row['WktsDescending'], row['Ground'], row['Start Date']
                ):
                    # Prepare data tuple
                    row_data = (
                        row['Player'],
                        row['Overs'],
                        row['Mdns'],
                        row['Runs'],
                        row['WktsDescending'],
                        row['Econ'],
                        row['Inns'],
                        row['Opposition'],
                        row['Ground'],
                        row['Start Date'],
                        row['Team']
                    )
                    
                    # Insert using database manager method
                    self.db_manager.insert_bowling_record(row_data)
                    inserted_count += 1
                    
                    if inserted_count % 10 == 0:
                        logger.info(f"Inserted {inserted_count} bowling records so far...")
                else:
                    duplicate_count += 1
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error inserting bowling row {index}: {e}")
                continue
        
        logger.info(f"Bowling insertion summary - Inserted: {inserted_count}, Duplicates: {duplicate_count}, Errors: {error_count}")
        return inserted_count