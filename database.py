import mysql.connector
import time
import logging
from datetime import datetime
from config import DB_CONFIG
import pandas as pd

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection with retry logic"""
        for attempt in range(10):
            try:
                # Close existing connection if any
                if self.connection:
                    try:
                        self.connection.close()
                    except:
                        pass
                
                # Create new connection with timeout settings
                connection_config = DB_CONFIG.copy()
                connection_config.update({
                    'connection_timeout': 60,
                    'autocommit': False,
                    'use_unicode': True,
                    'charset': 'utf8mb4'
                })
                
                self.connection = mysql.connector.connect(**connection_config)
                self.cursor = self.connection.cursor()
                logger.info("Database connection established successfully")
                return self.connection, self.cursor
                
            except mysql.connector.Error as err:
                logger.error(f"Database connection failed (attempt {attempt + 1}): {err}")
                if attempt < 9:
                    time.sleep(5)
                else:
                    raise Exception("Database connection failed after multiple attempts")
    
    def get_connection(self):
        """Get active database connection, reconnect if needed"""
        try:
            # Test if connection is still alive
            if self.connection and self.connection.is_connected():
                # Test the connection with a simple query
                self.cursor.execute("SELECT 1")
                self.cursor.fetchone()
                return self.connection, self.cursor
        except Exception as e:
            logger.warning(f"Connection test failed, reconnecting: {e}")
        
        # Connection is dead or doesn't exist, reconnect
        return self.connect()
    
    def fetch_latest_date(self, table_name):
        """Fetch the latest 'Start Date' from the specified table"""
        try:
            _, cursor = self.get_connection()
            query = f"SELECT MAX(`Start Date`) FROM {table_name}"
            cursor.execute(query)
            latest_date = cursor.fetchone()[0]
            
            if latest_date:
                if isinstance(latest_date, datetime):
                    return latest_date
                else:
                    latest_date_str = str(latest_date).split()[0]
                    return datetime.strptime(latest_date_str, "%Y-%m-%d")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching latest date from {table_name}: {e}")
            return None
    
    def fetch_unique_batting_players(self):
        """Fetch all unique player names from the batting table."""
        try:
            connection, cursor = self.get_connection()
            query = "SELECT DISTINCT `Player` FROM batting ORDER BY `Player`"
            cursor.execute(query)
            players = [row[0] for row in cursor.fetchall()]
            logger.info(f"Fetched {len(players)} unique batting players")
            return players
        except Exception as e:
            logger.error(f"Error fetching unique batting players: {e}")
            return []
    
    def fetch_unique_bowling_players(self):
        """Fetch all unique player names from the bowling table."""
        try:
            connection, cursor = self.get_connection()
            query = "SELECT DISTINCT `Player` FROM bowling ORDER BY `Player`"
            cursor.execute(query)
            players = [row[0] for row in cursor.fetchall()]
            logger.info(f"Fetched {len(players)} unique bowling players")
            return players
        except Exception as e:
            logger.error(f"Error fetching unique bowling players: {e}")
            return []
            
    def fetch_batting_data_by_player(self, player_name):
        """Fetch batting records for a specific player."""
        try:
            connection, cursor = self.get_connection()
            query = "SELECT `Player`, `RunsDescending`, `SR`, `Opposition`, `Start Date` FROM batting WHERE `Player` = %s"
            cursor.execute(query, (player_name,))
            
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            
            df = pd.DataFrame(rows, columns=cols)
            return df
        except Exception as e:
            logger.error(f"Error fetching batting data for player {player_name}: {e}")
            return None

    def fetch_bowling_data_by_player(self, player_name):
        """Fetch bowling records for a specific player."""
        try:
            connection, cursor = self.get_connection()
            query = "SELECT `Player`, `WktsDescending`, `Runs`, `Econ`, `Opposition`, `Start Date` FROM bowling WHERE `Player` = %s"
            cursor.execute(query, (player_name,))
            
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            
            df = pd.DataFrame(rows, columns=cols)
            return df
        except Exception as e:
            logger.error(f"Error fetching bowling data for player {player_name}: {e}")
            return None
    
    def row_exists_team(self, team, score_descending, ground, start_date):
        """Check if a team record already exists in the database"""
        try:
            _, cursor = self.get_connection()
            query = """
            SELECT COUNT(*) FROM team
            WHERE `Team` = %s AND `ScoreDescending` = %s AND `Ground` = %s AND `Start Date` = %s
            """
            cursor.execute(query, (team, score_descending, ground, start_date))
            count = cursor.fetchone()[0]
            exists = count > 0
            if exists:
                logger.debug(f"Team record exists: {team} - {score_descending} at {ground} on {start_date}")
            return exists
        except Exception as e:
            logger.error(f"Error checking team record existence: {e}")
            return False
    
    def row_exists_batting(self, player, runs_descending, ground, start_date):
        """Check if a batting record already exists in the database"""
        try:
            _, cursor = self.get_connection()
            query = """
            SELECT COUNT(*) FROM batting
            WHERE `Player` = %s AND `RunsDescending` = %s AND `Ground` = %s AND `Start Date` = %s
            """
            cursor.execute(query, (player, runs_descending, ground, start_date))
            count = cursor.fetchone()[0]
            exists = count > 0
            if exists:
                logger.debug(f"Batting record exists: {player} - {runs_descending} at {ground} on {start_date}")
            return exists
        except Exception as e:
            logger.error(f"Error checking batting record existence: {e}")
            return False
    
    def row_exists_bowling(self, player, overs, mdns, runs, wkts_descending, ground, start_date):
        """Check if a bowling record already exists in the database"""
        try:
            _, cursor = self.get_connection()
            query = """
            SELECT COUNT(*) FROM bowling
            WHERE `Player` = %s AND `Overs` = %s AND `Mdns` = %s AND `Runs` = %s 
            AND `WktsDescending` = %s AND `Ground` = %s AND `Start Date` = %s
            """
            cursor.execute(query, (player, overs, mdns, runs, wkts_descending, ground, start_date))
            count = cursor.fetchone()[0]
            exists = count > 0
            if exists:
                logger.debug(f"Bowling record exists: {player} - {overs} overs at {ground} on {start_date}")
            return exists
        except Exception as e:
            logger.error(f"Error checking bowling record existence: {e}")
            return False
    
    def get_record_counts(self):
        """Get record counts for all tables for debugging"""
        try:
            _, cursor = self.get_connection()
            
            counts = {}
            for table in ['team', 'batting', 'bowling']:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]
            
            logger.info(f"Database record counts: {counts}")
            return counts
            
        except Exception as e:
            logger.error(f"Error getting record counts: {e}")
            return {}
    
    def insert_team_record(self, row_data):
        """Insert a single team record"""
        try:
            connection, cursor = self.get_connection()
            query = """
            INSERT INTO team (`Team`, `ScoreDescending`, `Overs`, `RPO`, `Lead`, `Inns`, 
                            `Result`, `Opposition`, `Ground`, `Start Date`, `Declared`, `Wickets`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, row_data)
            connection.commit()
            logger.debug(f"Inserted team record: {row_data[0]} - {row_data[1]}")
        except Exception as e:
            logger.error(f"Error inserting team record: {e}")
            raise
    
    def insert_batting_record(self, row_data):
        """Insert a single batting record"""
        try:
            connection, cursor = self.get_connection()
            query = """
            INSERT INTO batting (`Player`, `RunsDescending`, `BF`, `4s`, `6s`, `SR`, `Inns`,
                               `Opposition`, `Ground`, `Start Date`, `Not Out`, `Team`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, row_data)
            connection.commit()
            logger.debug(f"Inserted batting record: {row_data[0]} - {row_data[1]}")
        except Exception as e:
            logger.error(f"Error inserting batting record: {e}")
            raise
    
    def insert_bowling_record(self, row_data):
        """Insert a single bowling record"""
        try:
            connection, cursor = self.get_connection()
            query = """
            INSERT INTO bowling (`Player`, `Overs`, `Mdns`, `Runs`, `WktsDescending`, `Econ`,
                               `Inns`, `Opposition`, `Ground`, `Start Date`, `Team`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, row_data)
            connection.commit()
            logger.debug(f"Inserted bowling record: {row_data[0]} - {row_data[1]} overs")
        except Exception as e:
            logger.error(f"Error inserting bowling record: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")