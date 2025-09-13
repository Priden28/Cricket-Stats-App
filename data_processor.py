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
    
    # Get connection ONCE like in your working example
    import mysql.connector
    from config import DB_CONFIG
    
    mydb = mysql.connector.connect(**DB_CONFIG)
    mycursor = mydb.cursor()
    
    inserted_count = 0
    duplicate_count = 0
    error_count = 0
    
    # Function to check if a row exists - exactly like your working example
    def row_exists(team, score_descending, ground, start_date):
        query = """
        SELECT COUNT(*) FROM team
        WHERE `Team` = %s AND `ScoreDescending` = %s AND `Ground` = %s AND `Start Date` = %s
        """
        mycursor.execute(query, (team, score_descending, ground, start_date))
        count = mycursor.fetchone()[0]
        return count > 0

    # Function to insert a row - exactly like your working example
    def insert_row(team, score_descending, overs, rpo, lead, inns, result, opposition, ground, start_date, declared, wickets):
        query = """
        INSERT INTO team (`Team`, `ScoreDescending`, `Overs`, `RPO`, `Lead`, `Inns`, 
                        `Result`, `Opposition`, `Ground`, `Start Date`, `Declared`, `Wickets`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        mycursor.execute(query, (team, score_descending, overs, rpo, lead, inns, result, opposition, ground, start_date, declared, wickets))
        mydb.commit()

    # Insert new data - exactly like your working example
    for index, row in df.iterrows():
        try:
            if not row_exists(row['Team'], row['ScoreDescending'], row['Ground'], row['Start Date']):
                insert_row(
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
                inserted_count += 1
                
                if inserted_count % 10 == 0:
                    logger.info(f"Inserted {inserted_count} team records so far...")
            else:
                duplicate_count += 1
                logger.debug(f"Row with Team '{row['Team']}', ScoreDescending '{row['ScoreDescending']}', Ground '{row['Ground']}', and Start Date '{row['Start Date']}' already exists.")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Error inserting team row {index}: {e}")
            logger.error(f"Row data: {dict(row)}")
            continue

    # Close the database connection
    mycursor.close()
    mydb.close()
    
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
    
    # Get connection ONCE like in your working example
    import mysql.connector
    from config import DB_CONFIG
    
    mydb = mysql.connector.connect(**DB_CONFIG)
    mycursor = mydb.cursor()
    
    inserted_count = 0
    duplicate_count = 0
    error_count = 0
    
    # Function to check if a row exists - exactly like your working example
    def row_exists(player, runs_descending, ground, start_date):
        query = """
        SELECT COUNT(*) FROM batting
        WHERE `Player` = %s AND `RunsDescending` = %s AND `Ground` = %s AND `Start Date` = %s
        """
        mycursor.execute(query, (player, runs_descending, ground, start_date))
        count = mycursor.fetchone()[0]
        return count > 0

    # Function to insert a row - exactly like your working example
    def insert_row(player, runs_descending, bf, fours, sixes, sr, inns, opposition, ground, start_date, not_out, team):
        query = """
        INSERT INTO batting (`Player`, `RunsDescending`, `BF`, `4s`, `6s`, `SR`, `Inns`, `Opposition`, `Ground`, `Start Date`, `Not Out`, `Team`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        mycursor.execute(query, (player, runs_descending, bf, fours, sixes, sr, inns, opposition, ground, start_date, not_out, team))
        mydb.commit()

    # Insert new data - exactly like your working example
    for index, row in df.iterrows():
        try:
            if not row_exists(row['Player'], row['RunsDescending'], row['Ground'], row['Start Date']):
                insert_row(
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
                inserted_count += 1
                
                if inserted_count % 10 == 0:
                    logger.info(f"Inserted {inserted_count} batting records so far...")
            else:
                duplicate_count += 1
                logger.debug(f"Row with Player '{row['Player']}', RunsDescending '{row['RunsDescending']}', Ground '{row['Ground']}', and Start Date '{row['Start Date']}' already exists.")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Error inserting batting row {index}: {e}")
            logger.error(f"Row data: {dict(row)}")
            continue

    # Close the database connection
    mycursor.close()
    mydb.close()
    
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

    # Get connection ONCE like in your working example
    import mysql.connector
    from config import DB_CONFIG
    
    mydb = mysql.connector.connect(**DB_CONFIG)
    mycursor = mydb.cursor()
    
    inserted_count = 0
    duplicate_count = 0
    error_count = 0
    
    # Function to check if a row exists - exactly like your working example
    def row_exists(player, overs, mdns, runs, wkts_descending, ground, start_date):
        query = """
        SELECT COUNT(*) FROM bowling
        WHERE `Player` = %s AND `Overs` = %s AND `Mdns` = %s AND `Runs` = %s 
        AND `WktsDescending` = %s AND `Ground` = %s AND `Start Date` = %s
        """
        mycursor.execute(query, (player, overs, mdns, runs, wkts_descending, ground, start_date))
        count = mycursor.fetchone()[0]
        return count > 0

    # Function to insert a row - exactly like your working example
    def insert_row(player, overs, mdns, runs, wkts_descending, econ, inns, opposition, ground, start_date, team):
        query = """
        INSERT INTO bowling (`Player`, `Overs`, `Mdns`, `Runs`, `WktsDescending`, `Econ`,
                           `Inns`, `Opposition`, `Ground`, `Start Date`, `Team`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        mycursor.execute(query, (player, overs, mdns, runs, wkts_descending, econ, inns, opposition, ground, start_date, team))
        mydb.commit()

    # Insert new data - exactly like your working example
    for index, row in df.iterrows():
        try:
            if not row_exists(row['Player'], row['Overs'], row['Mdns'], row['Runs'], 
                            row['WktsDescending'], row['Ground'], row['Start Date']):
                insert_row(
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
                inserted_count += 1
                
                if inserted_count % 10 == 0:
                    logger.info(f"Inserted {inserted_count} bowling records so far...")
            else:
                duplicate_count += 1
                logger.debug(f"Row with Player '{row['Player']}', Overs '{row['Overs']}', Ground '{row['Ground']}', and Start Date '{row['Start Date']}' already exists.")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Error inserting bowling row {index}: {e}")
            logger.error(f"Row data: {dict(row)}")
            continue

    # Close the database connection
    mycursor.close()
    mydb.close()
    
    logger.info(f"Bowling data processing complete:")
    logger.info(f"  - Inserted: {inserted_count} new rows")
    logger.info(f"  - Duplicates skipped: {duplicate_count}")
    logger.info(f"  - Errors: {error_count}")
    logger.info(f"  - Total processed: {len(df)}")
    
    return df