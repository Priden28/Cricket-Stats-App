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
            if isinstance(overs, str):
                overs = overs.strip()
                if '.' in overs:
                    overs_parts = overs.split('.')
                    overs_in_float = int(overs_parts[0]) + (int(overs_parts[1]) / 6)
                    return overs_in_float
                else:
                    return float(overs)
            return float('nan')

        dataframe[column_name] = dataframe[column_name].apply(convert_overs_to_float)
        return dataframe
    
    def normalize_start_date(self, date_value):
        """Normalize Start Date to date only (strip time component)"""
        if pd.isna(date_value):
            return None
        
        if isinstance(date_value, str):
            try:
                # Parse the datetime string and extract only the date part
                dt = pd.to_datetime(date_value)
                return dt.date()
            except:
                return None
        elif hasattr(date_value, 'date'):
            # If it's already a datetime object, extract the date
            return date_value.date()
        else:
            return date_value
    
    def process_team_data(self, scraped_data, columns):
        """Process and store team data in the database"""
        logger.info(f"Starting to process team data with {len(scraped_data)} scraped rows")
        
        cleaned_data = self.clean_data(scraped_data, len(columns))
        df = self.convert_to_dataframe(cleaned_data, columns)
        df = self.clean_opposition_column(df)
        df = self.process_team_score(df)
        df = self.process_overs_column(df, column_name='Overs')
        
        # Convert data types like in original code
        df['RPO'] = df['RPO'].astype(float)
        df['Inns'] = df['Inns'].astype(int)
        df['Lead'] = df['Lead'].fillna(0).astype(int)
        df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
        
        # Normalize Start Date to remove time component
        df['Start Date'] = df['Start Date'].apply(self.normalize_start_date)
        
        # Log the final DataFrame structure
        logger.info(f"Final DataFrame columns: {list(df.columns)}")
        logger.info(f"DataFrame shape: {df.shape}")
        
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        # Process each row individually
        for index, row in df.iterrows():
            try:
                # Get fresh connection for each check and insert
                connection, cursor = self.db_manager.get_connection()
                
                if not self.db_manager.row_exists_team(
                    row['Team'], row['ScoreDescending'], row['Ground'], row['Start Date']
                ):
                    # Insert single row
                    query = """
                    INSERT INTO team (`Team`, `ScoreDescending`, `Overs`, `RPO`, `Lead`, `Inns`, 
                                    `Result`, `Opposition`, `Ground`, `Start Date`, `Declared`, `Wickets`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(query, (
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
                    ))
                    connection.commit()
                    inserted_count += 1
                    
                    if inserted_count % 10 == 0:  # Log every 10 insertions
                        logger.info(f"Inserted {inserted_count} team records so far...")
                else:
                    duplicate_count += 1
                    logger.debug(f"Duplicate team record skipped: {row['Team']} - {row['ScoreDescending']} at {row['Ground']}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error inserting team row {index}: {e}")
                logger.error(f"Row data: {dict(row)}")
                
                # Try to rollback and continue
                try:
                    connection.rollback()
                except:
                    pass
                continue
        
        logger.info(f"Team data processing complete:")
        logger.info(f"  - Inserted: {inserted_count} new rows")
        logger.info(f"  - Duplicates skipped: {duplicate_count}")
        logger.info(f"  - Errors: {error_count}")
        logger.info(f"  - Total processed: {len(df)}")
        
        return df

    def process_batting_data(self, scraped_data, columns):
        """Process and store batting data in the database"""
        logger.info(f"Starting to process batting data with {len(scraped_data)} scraped rows")
        
        cleaned_data = self.clean_data(scraped_data, len(columns))
        df = self.convert_to_dataframe(cleaned_data, columns)
        df = self.clean_opposition_column(df)
        df = self.split_player_and_team(df, TEAM_MAPPING)
        
        # Clean and convert data like in original code
        df = df[~df['RunsDescending'].isin(['DNB', 'absent', 'sub'])]
        df['Not Out'] = df['RunsDescending'].apply(lambda x: 1 if '*' in str(x) else 0)
        df['RunsDescending'] = df['RunsDescending'].str.replace('*', '', regex=False)
        df = df.drop(columns=['Mins'])
        df['RunsDescending'] = df['RunsDescending'].astype(int)
        df['4s'] = df['4s'].astype(int)
        df['6s'] = df['6s'].astype(int)
        df['BF'] = df['BF'].astype(int)
        df['Inns'] = df['Inns'].astype(int)
        df['SR'] = df['SR'].str.strip().replace('-', '0').astype(float)
        df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
        
        # Normalize Start Date to remove time component
        df['Start Date'] = df['Start Date'].apply(self.normalize_start_date)
        
        # Log the final DataFrame structure
        logger.info(f"Final batting DataFrame columns: {list(df.columns)}")
        logger.info(f"Batting DataFrame shape: {df.shape}")
        
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        # Process each row individually
        for index, row in df.iterrows():
            try:
                # Get fresh connection for each check and insert
                connection, cursor = self.db_manager.get_connection()
                
                if not self.db_manager.row_exists_batting(
                    row['Player'], row['RunsDescending'], row['Ground'], row['Start Date']
                ):
                    # Insert single row
                    query = """
                    INSERT INTO batting (`Player`, `RunsDescending`, `BF`, `4s`, `6s`, `SR`, `Inns`,
                                       `Opposition`, `Ground`, `Start Date`, `Not Out`, `Team`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(query, (
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
                    ))
                    connection.commit()
                    inserted_count += 1
                    
                    if inserted_count % 10 == 0:  # Log every 10 insertions
                        logger.info(f"Inserted {inserted_count} batting records so far...")
                else:
                    duplicate_count += 1
                    logger.debug(f"Duplicate batting record skipped: {row['Player']} - {row['RunsDescending']} at {row['Ground']}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error inserting batting row {index}: {e}")
                logger.error(f"Row data: {dict(row)}")
                
                # Try to rollback and continue
                try:
                    connection.rollback()
                except:
                    pass
                continue
        
        logger.info(f"Batting data processing complete:")
        logger.info(f"  - Inserted: {inserted_count} new rows")
        logger.info(f"  - Duplicates skipped: {duplicate_count}")
        logger.info(f"  - Errors: {error_count}")
        logger.info(f"  - Total processed: {len(df)}")
        
        return df

    def process_bowling_data(self, scraped_data, columns):
        """Process and store bowling data in the database"""
        logger.info(f"Starting to process bowling data with {len(scraped_data)} scraped rows")
        
        cleaned_data = self.clean_data(scraped_data, len(columns))
        df = self.convert_to_dataframe(cleaned_data, columns)
        df = self.clean_opposition_column(df)
        df = self.split_player_and_team(df, TEAM_MAPPING)
        
        # Clean and convert data like in original code - FILTER OUT DNB/absent FIRST
        df = df[~df['WktsDescending'].isin(['DNB', 'absent', 'sub'])]
        # Also filter out DNB from Overs column before processing
        df = df[~df['Overs'].isin(['DNB', 'absent', 'sub'])]
        df = self.process_overs_column(df, column_name='Overs')
        
        # Handle Inns column more carefully
        try:
            df['Inns'] = df['Inns'].astype(int)
        except (ValueError, TypeError):
            df['Inns'] = df['Inns'].apply(lambda x: int(str(x).split()[0]) if str(x).split()[0].isdigit() else 1)
        
        df['Mdns'] = df['Mdns'].replace('-', '0').astype(int)
        df['Runs'] = df['Runs'].replace('-', '0').astype(int)
        df['WktsDescending'] = df['WktsDescending'].replace('-', '0').astype(int)
        df['Econ'] = df['Econ'].replace('-', '0').astype(float)
        df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')
        
        # Normalize Start Date to remove time component
        df['Start Date'] = df['Start Date'].apply(self.normalize_start_date)

        # Log the final DataFrame structure
        logger.info(f"Final bowling DataFrame columns: {list(df.columns)}")
        logger.info(f"Bowling DataFrame shape: {df.shape}")

        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        # Process each row individually
        for index, row in df.iterrows():
            try:
                # Get fresh connection for each check and insert
                connection, cursor = self.db_manager.get_connection()
                
                if not self.db_manager.row_exists_bowling(
                    row['Player'], row['Overs'], row['Mdns'], row['Runs'], 
                    row['WktsDescending'], row['Ground'], row['Start Date']
                ):
                    # Insert single row
                    query = """
                    INSERT INTO bowling (`Player`, `Overs`, `Mdns`, `Runs`, `WktsDescending`, `Econ`,
                                       `Inns`, `Opposition`, `Ground`, `Start Date`, `Team`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(query, (
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
                    ))
                    connection.commit()
                    inserted_count += 1
                    
                    if inserted_count % 10 == 0:  # Log every 10 insertions
                        logger.info(f"Inserted {inserted_count} bowling records so far...")
                else:
                    duplicate_count += 1
                    logger.debug(f"Duplicate bowling record skipped: {row['Player']} - {row['Overs']} overs at {row['Ground']}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error inserting bowling row {index}: {e}")
                logger.error(f"Row data: {dict(row)}")
                
                # Try to rollback and continue
                try:
                    connection.rollback()
                except:
                    pass
                continue
        
        logger.info(f"Bowling data processing complete:")
        logger.info(f"  - Inserted: {inserted_count} new rows")
        logger.info(f"  - Duplicates skipped: {duplicate_count}")
        logger.info(f"  - Errors: {error_count}")
        logger.info(f"  - Total processed: {len(df)}")
        
        return df