import logging
import pandas as pd
from collections import Counter
import hashlib
from sklearn.preprocessing import LabelEncoder
import plotly.express as px
import numpy as np

from database import DatabaseManager

logger = logging.getLogger(__name__)

class AnalyticsService:
    """
    A service to perform data analytics and generate plots.
    """
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.match_id_generator = self.MatchIDGenerator()
        self.label_encoder = LabelEncoder()

    class MatchIDGenerator:
        """Helper class to generate and store match IDs."""
        def __init__(self):
            self.match_id_dict = {}

        def generate_match_id(self, row):
            # Extract only the date part to ignore time component
            # Handle both datetime objects and strings
            start_date_obj = row['Start Date']
            if isinstance(start_date_obj, str):
                start_date_obj = pd.to_datetime(start_date_obj)
            start_date = start_date_obj.strftime('%Y-%m-%d')
            match_info = f"{row['Ground']}_{start_date}"
            match_id = hashlib.sha256(match_info.encode()).hexdigest()
            self.match_id_dict[match_id] = (row['Ground'], row['Start Date'])
            return match_id

    def fetch_data_from_db(self):
        """Fetches all necessary data from the database."""
        try:
            connection, cursor = self.db_manager.get_connection()
            
            # Fetch team data
            cursor.execute("SELECT * FROM team")
            team_columns = [i[0] for i in cursor.description]
            df_team = pd.DataFrame(cursor.fetchall(), columns=team_columns)
            logger.info(f"Fetched {len(df_team)} rows from team table")
            
            # Fetch batting data
            cursor.execute("SELECT * FROM batting")
            batting_columns = [i[0] for i in cursor.description]
            df_batting = pd.DataFrame(cursor.fetchall(), columns=batting_columns)
            logger.info(f"Fetched {len(df_batting)} rows from batting table")
            
            # Debug: Print unique players
            if not df_batting.empty:
                unique_players = df_batting['Player'].unique()
                logger.info(f"Unique players in batting table: {len(unique_players)}")
                logger.info(f"First 10 players: {unique_players[:10]}")
            
            # Fetch bowling data
            cursor.execute("SELECT * FROM bowling")
            bowling_columns = [i[0] for i in cursor.description]
            df_bowling = pd.DataFrame(cursor.fetchall(), columns=bowling_columns)
            logger.info(f"Fetched {len(df_bowling)} rows from bowling table")

            return df_team, df_batting, df_bowling
        except Exception as e:
            logger.error(f"Error fetching data from database: {e}")
            raise

    def process_data(self, df_team, df_batting, df_bowling):
        """Processes and prepares the data for analysis."""
        
        # Team data processing
        df_team['ScoreDescending'] = pd.to_numeric(df_team['ScoreDescending'], errors='coerce').astype('Int64')
        df_team['Wickets'] = df_team['Wickets'].astype('Int64')
        df_team['Start Date'] = pd.to_datetime(df_team['Start Date'])

        # Find and assign hosts
        ground_teams_count = {}
        for _, row in df_team.iterrows():
            ground = row['Ground']
            teams = [row['Team'], row['Opposition']]
            if ground not in ground_teams_count:
                ground_teams_count[ground] = Counter(teams)
            else:
                ground_teams_count[ground] += Counter(teams)

        df_team['Host'] = df_team['Ground'].apply(
            lambda ground: ground_teams_count[ground].most_common(1)[0][0] if ground in ground_teams_count else ''
        )
        df_team = df_team[df_team['Start Date'] >= '1985-01-01']

        # Batting and bowling data processing
        def process_cricket_data(df):
            if 'Country' in df.columns:
                df.rename(columns={'Country': 'Team'}, inplace=True)
            df['Start Date'] = pd.to_datetime(df['Start Date'])
            df = df[df['Start Date'] >= '1985-01-01']
            return df

        df_batting = process_cricket_data(df_batting)
        df_bowling = process_cricket_data(df_bowling)
        
        logger.info(f"After processing, batting data has {len(df_batting)} rows")
        logger.info(f"After processing, bowling data has {len(df_bowling)} rows")
        
        # Generate and encode Match IDs
        df_team['Match ID'] = df_team.apply(self.match_id_generator.generate_match_id, axis=1)
        df_batting['Match ID'] = df_batting.apply(self.match_id_generator.generate_match_id, axis=1)
        df_bowling['Match ID'] = df_bowling.apply(self.match_id_generator.generate_match_id, axis=1)
        
        all_match_ids = pd.concat([df_team['Match ID'], df_batting['Match ID'], df_bowling['Match ID']], axis=0)
        self.label_encoder.fit(all_match_ids)

        df_team['NumericMatchID'] = self.label_encoder.transform(df_team['Match ID'])
        df_batting['NumericMatchID'] = self.label_encoder.transform(df_batting['Match ID'])
        df_bowling['NumericMatchID'] = self.label_encoder.transform(df_bowling['Match ID'])
        
        # Drop Match ID columns
        df_team = df_team.drop('Match ID', axis=1)
        df_batting = df_batting.drop('Match ID', axis=1)
        df_bowling = df_bowling.drop('Match ID', axis=1)
        
        # Clean batting data
        df_batting.drop(df_batting[(df_batting['RunsDescending'] == 0) & (df_batting['BF'] == 0)].index, inplace=True)
        # Clean bowling data - remove entries with no wickets and no runs conceded
        df_bowling.drop(df_bowling[(df_bowling['WktsDescending'] == 0) & (df_bowling['Runs'] == 0)].index, inplace=True)
        
        df_team['Outcome'] = df_team['Result'].map({'won': 'Win', 'lost': 'Loss', 'draw': 'Draw'})

        logger.info(f"Final processed batting data has {len(df_batting)} rows")
        logger.info(f"Final processed bowling data has {len(df_bowling)} rows")
        return df_team, df_batting, df_bowling

    def analyze_batting_by_country(self, player_name):
        """
        Analyze a player's batting performance by country.
        Returns batting statistics grouped by host country.
        """
        try:
            logger.info(f"Analyzing batting by country for player: {player_name}")
            df_team, df_batting, df_bowling = self.process_data(*self.fetch_data_from_db())

            # Check if player exists
            if player_name not in df_batting['Player'].values:
                available_players = df_batting['Player'].unique()
                logger.error(f"Player '{player_name}' not found. Available players: {available_players[:10]}")
                return {
                    'error': f"Player '{player_name}' not found in batting data",
                    'player': player_name
                }

            # Filter data for the player
            player_data = df_batting[df_batting['Player'] == player_name]

            if player_data.empty:
                return {
                    'error': f"No batting data found for {player_name}",
                    'player': player_name
                }

            # Get player's team
            player_team = player_data.iloc[0]['Team']

            # Create dictionaries to store totals for each country
            country_totals = {}
            country_counts = {}
            country_matches = {}

            # Iterate through each match the player played
            for index, match in player_data.iterrows():
                match_id = match['NumericMatchID']
                
                # Find the host country for the match
                host_matches = df_team[df_team['NumericMatchID'] == match_id]
                if host_matches.empty:
                    continue
                    
                host_country = host_matches['Host'].iloc[0]
                
                # Initialize country data if not exists
                if host_country not in country_totals:
                    country_totals[host_country] = 0
                    country_counts[host_country] = 0
                    country_matches[host_country] = set()

                # Add runs and track dismissals
                country_totals[host_country] += match['RunsDescending']
                if not match['Not Out']:
                    country_counts[host_country] += 1
                
                # Track unique matches
                country_matches[host_country].add(match_id)

            # Calculate batting average for each country
            country_statistics = []
            for country in country_totals:
                if country_counts[country] > 0:
                    batting_average = country_totals[country] / country_counts[country]
                else:
                    batting_average = 0

                country_statistics.append({
                    'country': country,
                    'batting_average': round(batting_average, 2),
                    'total_runs': int(country_totals[country]),
                    'times_out': int(country_counts[country]),
                    'matches_played': len(country_matches[country])
                })

            # Sort by batting average (highest to lowest)
            country_statistics.sort(key=lambda x: x['batting_average'], reverse=True)

            result = {
                'player': player_name,
                'player_team': player_team,
                'total_countries': len(country_statistics),
                'country_statistics': country_statistics
            }

            logger.info(f"Batting by country analysis completed for {player_name}")
            return result

        except Exception as e:
            logger.error(f"Error analyzing batting by country for {player_name}: {e}")
            return {
                'error': f"Failed to analyze batting by country: {str(e)}",
                'player': player_name
            }

    def analyze_bowling_by_country(self, player_name):
        """
        Analyze a player's bowling performance by country.
        Returns bowling statistics grouped by host country.
        """
        try:
            logger.info(f"Analyzing bowling by country for player: {player_name}")
            df_team, df_batting, df_bowling = self.process_data(*self.fetch_data_from_db())

            # Check if player exists
            if player_name not in df_bowling['Player'].values:
                available_players = df_bowling['Player'].unique()
                logger.error(f"Player '{player_name}' not found in bowling data. Available players: {available_players[:10]}")
                return {
                    'error': f"Player '{player_name}' not found in bowling data",
                    'player': player_name
                }

            # Filter data for the player
            player_data = df_bowling[df_bowling['Player'] == player_name]

            if player_data.empty:
                return {
                    'error': f"No bowling data found for {player_name}",
                    'player': player_name
                }

            # Get player's team
            player_team = player_data.iloc[0]['Team']

            # Create dictionaries to store totals for each country
            country_runs = {}
            country_wickets = {}
            country_matches = {}

            # Iterate through each match the player played
            for index, match in player_data.iterrows():
                match_id = match['NumericMatchID']
                
                # Find the host country for the match
                host_matches = df_team[df_team['NumericMatchID'] == match_id]
                if host_matches.empty:
                    continue
                    
                host_country = host_matches['Host'].iloc[0]
                
                # Initialize country data if not exists
                if host_country not in country_runs:
                    country_runs[host_country] = 0
                    country_wickets[host_country] = 0
                    country_matches[host_country] = set()

                # Add runs and wickets
                country_runs[host_country] += match['Runs']
                country_wickets[host_country] += match['WktsDescending']
                
                # Track unique matches
                country_matches[host_country].add(match_id)

            # Calculate bowling average for each country
            country_statistics = []
            for country in country_runs:
                if country_wickets[country] > 0:
                    bowling_average = country_runs[country] / country_wickets[country]
                else:
                    bowling_average = None  # No wickets taken

                country_statistics.append({
                    'country': country,
                    'bowling_average': round(bowling_average, 2) if bowling_average is not None else None,
                    'total_runs_conceded': int(country_runs[country]),
                    'total_wickets': int(country_wickets[country]),
                    'matches_played': len(country_matches[country])
                })

            # Sort by bowling average (lowest to highest, with None values at the end)
            # Lower bowling average is better
            country_statistics.sort(key=lambda x: (x['bowling_average'] is None, x['bowling_average'] or float('inf')))

            result = {
                'player': player_name,
                'player_team': player_team,
                'total_countries': len(country_statistics),
                'country_statistics': country_statistics
            }

            logger.info(f"Bowling by country analysis completed for {player_name}")
            return result

        except Exception as e:
            logger.error(f"Error analyzing bowling by country for {player_name}: {e}")
            return {
                'error': f"Failed to analyze bowling by country: {str(e)}",
                'player': player_name
            }

    def analyze_batsman_vs_bowler(self, batsman_name, bowler_name=None):
        """
        Analyze batsman's performance against a specific bowler or opposition team.
        Returns batting averages and comparison statistics.
        """
        try:
            logger.info(f"Analyzing batsman vs bowler: {batsman_name} vs {bowler_name}")
            df_team, df_batting, df_bowling = self.process_data(*self.fetch_data_from_db())

            # Check if batsman exists
            if batsman_name not in df_batting['Player'].values:
                available_players = df_batting['Player'].unique()
                logger.error(f"Batsman '{batsman_name}' not found. Available players: {available_players[:10]}")
                return {
                    'error': f"Batsman '{batsman_name}' not found in batting data",
                    'batsman': batsman_name,
                    'bowler': bowler_name
                }

            # Filter batting DataFrame for matches where the batsman played
            df_batsman = df_batting[df_batting['Player'] == batsman_name]

            if df_batsman.empty:
                return {
                    'error': f"No batting data found for {batsman_name}",
                    'batsman': batsman_name,
                    'bowler': bowler_name
                }

            # Get batsman's team
            batsman_team = df_batsman.iloc[0]['Team']

            # Handle bowler analysis
            bowler_team = None
            if bowler_name:
                # Check if bowler exists
                if bowler_name not in df_bowling['Player'].values:
                    available_bowlers = df_bowling['Player'].unique()
                    logger.error(f"Bowler '{bowler_name}' not found. Available bowlers: {available_bowlers[:10]}")
                    return {
                        'error': f"Bowler '{bowler_name}' not found in bowling data",
                        'batsman': batsman_name,
                        'bowler': bowler_name
                    }
                
                # Get bowler's team
                bowler_team = df_bowling[df_bowling['Player'] == bowler_name]['Team'].iloc[0]

            # Filter batting DataFrame for matches against the bowler's team (if provided)
            if bowler_team:
                df_batsman_opposition = df_batsman[df_batsman['Opposition'] == bowler_team]
            else:
                df_batsman_opposition = df_batsman.copy()

            # Calculate overall batting average against the opposition
            total_runs_opposition = df_batsman_opposition['RunsDescending'].sum()
            total_outs_opposition = (1 - df_batsman_opposition['Not Out']).sum()
            batsman_average_overall = total_runs_opposition / total_outs_opposition if total_outs_opposition > 0 else 0

            # Initialize variables for analysis
            batsman_average_with_bowler = None
            batsman_average_without_bowler = 0
            matches_with_bowler = 0
            matches_without_bowler = len(df_batsman_opposition)
            total_runs_with_bowler = 0
            total_runs_without_bowler = total_runs_opposition
            total_outs_with_bowler = 0
            total_outs_without_bowler = total_outs_opposition

            # Filter for matches where the bowler was present (if applicable)
            if bowler_name and bowler_team:
                # Get match IDs where the bowler played
                bowler_match_ids = df_bowling[df_bowling['Player'] == bowler_name]['NumericMatchID'].unique()
                
                # Create boolean mask for matches with bowler
                matches_with_bowler_mask = df_batsman_opposition['NumericMatchID'].isin(bowler_match_ids)
                
                # Split data into matches with and without bowler
                df_batsman_with_bowler = df_batsman_opposition[matches_with_bowler_mask]
                df_batsman_without_bowler = df_batsman_opposition[~matches_with_bowler_mask]

                # Calculate batting average with the bowler
                if not df_batsman_with_bowler.empty:
                    total_runs_with_bowler = df_batsman_with_bowler['RunsDescending'].sum()
                    total_outs_with_bowler = (1 - df_batsman_with_bowler['Not Out']).sum()
                    batsman_average_with_bowler = total_runs_with_bowler / total_outs_with_bowler if total_outs_with_bowler > 0 else 0
                    matches_with_bowler = len(df_batsman_with_bowler)

                # Calculate batting average without the bowler
                if not df_batsman_without_bowler.empty:
                    total_runs_without_bowler = df_batsman_without_bowler['RunsDescending'].sum()
                    total_outs_without_bowler = (1 - df_batsman_without_bowler['Not Out']).sum()
                    batsman_average_without_bowler = total_runs_without_bowler / total_outs_without_bowler if total_outs_without_bowler > 0 else 0
                    matches_without_bowler = len(df_batsman_without_bowler)
                else:
                    total_runs_without_bowler = 0
                    total_outs_without_bowler = 0
                    batsman_average_without_bowler = 0
                    matches_without_bowler = 0

            result = {
                'batsman': batsman_name,
                'batsman_team': batsman_team,
                'bowler': bowler_name,
                'bowler_team': bowler_team,
                'overall_average': round(batsman_average_overall, 2),
                'average_with_bowler': round(batsman_average_with_bowler, 2) if batsman_average_with_bowler is not None else None,
                'average_without_bowler': round(batsman_average_without_bowler, 2),
                'total_matches_vs_opposition': len(df_batsman_opposition),
                'matches_with_bowler': matches_with_bowler,
                'matches_without_bowler': matches_without_bowler,
                'total_runs_overall': int(total_runs_opposition),
                'total_runs_with_bowler': int(total_runs_with_bowler),
                'total_runs_without_bowler': int(total_runs_without_bowler),
                'total_outs_overall': int(total_outs_opposition),
                'total_outs_with_bowler': int(total_outs_with_bowler),
                'total_outs_without_bowler': int(total_outs_without_bowler),
                'analysis_type': 'with_bowler' if bowler_name else 'overall_only'
            }

            logger.info(f"Batsman vs bowler analysis completed for {batsman_name} vs {bowler_name}")
            return result

        except Exception as e:
            logger.error(f"Error analyzing batsman vs bowler for {batsman_name} vs {bowler_name}: {e}")
            return {
                'error': f"Failed to analyze batsman vs bowler: {str(e)}",
                'batsman': batsman_name,
                'bowler': bowler_name
            }

    def analyze_batting_match_outcomes(self, player_name, min_score):
        """
        Analyze team match outcomes when a player scores above a minimum threshold.
        Returns match outcome statistics and percentages.
        """
        try:
            logger.info(f"Analyzing batting match outcomes for {player_name} with minimum score {min_score}")
            df_team, df_batting, df_bowling = self.process_data(*self.fetch_data_from_db())

            # Check if player exists
            if player_name not in df_batting['Player'].values:
                available_players = df_batting['Player'].unique()
                logger.error(f"Player '{player_name}' not found. Available players: {available_players[:10]}")
                return None

            # Step 1: Filter df_batting for player and minimum score
            player_matches = df_batting[(df_batting['Player'] == player_name) & (df_batting['RunsDescending'] >= min_score)]
            
            if player_matches.empty:
                return {
                    'error': f"No matches found for {player_name} with runs greater than or equal to {min_score}",
                    'player': player_name,
                    'min_score': min_score,
                    'total_matches': 0
                }

            # Step 2: Extract unique matches where the player scored the minimum score or more
            player_matches_ids = player_matches['NumericMatchID'].unique()

            # Step 3: Filter df_team for matches where the player's team played
            team_matches = df_team[df_team['NumericMatchID'].isin(player_matches_ids)]

            # Get the player's team from the first match
            player_team = player_matches.iloc[0]['Team']

            # Step 4: Count the number of matches won, lost, and drawn by the player's team
            team_won_matches_count = team_matches[
                (team_matches['Team'] == player_team) & (team_matches['Result'] == 'won')
            ]['NumericMatchID'].nunique()
            
            team_lost_matches_count = team_matches[
                (team_matches['Team'] == player_team) & (team_matches['Result'] == 'lost')
            ]['NumericMatchID'].nunique()
            
            team_drawn_matches_count = team_matches[
                (team_matches['Team'] == player_team) & (team_matches['Result'] == 'draw')
            ]['NumericMatchID'].nunique()

            # Step 5: Calculate percentages
            total_matches = len(player_matches_ids)
            winning_percentage = (team_won_matches_count / total_matches) * 100
            losing_percentage = (team_lost_matches_count / total_matches) * 100
            drawing_percentage = (team_drawn_matches_count / total_matches) * 100

            result = {
                'player': player_name,
                'team': player_team,
                'min_score': min_score,
                'total_matches': total_matches,
                'matches_won': team_won_matches_count,
                'matches_lost': team_lost_matches_count,
                'matches_drawn': team_drawn_matches_count,
                'winning_percentage': round(winning_percentage, 2),
                'losing_percentage': round(losing_percentage, 2),
                'drawing_percentage': round(drawing_percentage, 2)
            }

            logger.info(f"Batting outcome analysis completed for {player_name}")
            return result

        except Exception as e:
            logger.error(f"Error analyzing batting match outcomes for {player_name}: {e}")
            return None

    def analyze_bowling_match_outcomes(self, player_name, min_wickets):
        """
        Analyze team match outcomes when a player takes above a minimum wickets threshold.
        Returns match outcome statistics and percentages.
        """
        try:
            logger.info(f"Analyzing bowling match outcomes for {player_name} with minimum wickets {min_wickets}")
            df_team, df_batting, df_bowling = self.process_data(*self.fetch_data_from_db())

            # Check if player exists
            if player_name not in df_bowling['Player'].values:
                available_players = df_bowling['Player'].unique()
                logger.error(f"Player '{player_name}' not found in bowling data. Available players: {available_players[:10]}")
                return None

            # Step 1: Filter df_bowling for player and minimum wickets
            player_matches = df_bowling[(df_bowling['Player'] == player_name) & (df_bowling['WktsDescending'] >= min_wickets)]
            
            if player_matches.empty:
                return {
                    'error': f"No matches found for {player_name} with wickets greater than or equal to {min_wickets}",
                    'player': player_name,
                    'min_wickets': min_wickets,
                    'total_matches': 0
                }

            # Step 2: Extract unique matches where the player took the minimum wickets or more
            player_matches_ids = player_matches['NumericMatchID'].unique()

            # Step 3: Filter df_team for matches where the player's team played
            team_matches = df_team[df_team['NumericMatchID'].isin(player_matches_ids)]

            # Get the player's team from the first match
            player_team = player_matches.iloc[0]['Team']

            # Step 4: Count the number of matches won, lost, and drawn by the player's team
            team_won_matches_count = team_matches[
                (team_matches['Team'] == player_team) & (team_matches['Result'] == 'won')
            ]['NumericMatchID'].nunique()
            
            team_lost_matches_count = team_matches[
                (team_matches['Team'] == player_team) & (team_matches['Result'] == 'lost')
            ]['NumericMatchID'].nunique()
            
            team_drawn_matches_count = team_matches[
                (team_matches['Team'] == player_team) & (team_matches['Result'] == 'draw')
            ]['NumericMatchID'].nunique()

            # Step 5: Calculate percentages
            total_matches = len(player_matches_ids)
            winning_percentage = (team_won_matches_count / total_matches) * 100
            losing_percentage = (team_lost_matches_count / total_matches) * 100
            drawing_percentage = (team_drawn_matches_count / total_matches) * 100

            result = {
                'player': player_name,
                'team': player_team,
                'min_wickets': min_wickets,
                'total_matches': total_matches,
                'matches_won': team_won_matches_count,
                'matches_lost': team_lost_matches_count,
                'matches_drawn': team_drawn_matches_count,
                'winning_percentage': round(winning_percentage, 2),
                'losing_percentage': round(losing_percentage, 2),
                'drawing_percentage': round(drawing_percentage, 2)
            }

            logger.info(f"Bowling outcome analysis completed for {player_name}")
            return result

        except Exception as e:
            logger.error(f"Error analyzing bowling match outcomes for {player_name}: {e}")
            return None

    def generate_player_batting_average_plot(self, player_name):
        """
        Generates a Plotly line chart for a player's cumulative batting average.
        Returns the figure as a JSON object.
        """
        try:
            logger.info(f"Generating batting plot for player: {player_name}")
            _, df_batting, _ = self.process_data(*self.fetch_data_from_db())

            # Check if player exists
            if player_name not in df_batting['Player'].values:
                available_players = df_batting['Player'].unique()
                logger.error(f"Player '{player_name}' not found. Available players: {available_players[:10]}")
                return None

            # Extract Year from Start Date and Sort Data - exactly like your local code
            df_batting['Year'] = df_batting['Start Date'].dt.year
            df_batting.sort_values(by=['Player', 'Year'], inplace=True)

            # Group by Player and Year, then calculate yearly statistics - exactly like your local code
            yearly_stats = df_batting.groupby(['Player', 'Year']).apply(lambda group: pd.Series({
                'Total Runs': group['RunsDescending'].sum(),
                'Outs': (1 - group['Not Out']).sum(),
                'Matches Played': group['NumericMatchID'].nunique(),
                'Highest Score': group['RunsDescending'].max()
            })).reset_index()

            logger.info(f"Generated yearly stats with {len(yearly_stats)} rows")

            # Calculate cumulative statistics using cumsum() and cummax() - exactly like your local code
            yearly_stats['Cumulative Runs'] = yearly_stats.groupby('Player')['Total Runs'].cumsum()
            yearly_stats['Cumulative Outs'] = yearly_stats.groupby('Player')['Outs'].cumsum()
            yearly_stats['Cumulative Matches Played'] = yearly_stats.groupby('Player')['Matches Played'].cumsum()
            yearly_stats['Cumulative Highest Score'] = yearly_stats.groupby('Player')['Highest Score'].cummax()

            # Calculate cumulative batting average - exactly like your local code
            yearly_stats['Cumulative Batting Average'] = yearly_stats['Cumulative Runs'] / yearly_stats['Cumulative Outs']

            # Filter DataFrame for the specified player - exactly like your local code
            df_player = yearly_stats[yearly_stats['Player'] == player_name]

            if df_player.empty:
                logger.error(f"No yearly stats found for player: {player_name}")
                return None

            logger.info(f"Found {len(df_player)} years of data for player: {player_name}")

            # Plot the cumulative batting average vs. years using Plotly Express - exactly like your local code
            fig = px.line(df_player, x='Year', y='Cumulative Batting Average', 
                   hover_data={'Cumulative Runs': True, 'Cumulative Matches Played': True, 'Cumulative Highest Score': True}, 
                   title=f'{player_name} Cumulative Batting Average Over the Years',
                   labels={'Cumulative Batting Average': 'Cumulative Batting Average', 'Year': 'Year'})

            # Fix for hover issue on last point - ensure all data points are properly accessible
            fig.update_traces(
                mode='lines+markers',
                marker=dict(size=4),
                line=dict(width=2)
            )
            
            # Improve hover behavior
            fig.update_layout(
                hovermode='x unified',
                xaxis=dict(
                    showgrid=True,
                    rangeslider=dict(visible=False),
                    type='linear'  # Ensure year is treated as continuous numeric
                ),
                yaxis=dict(
                    showgrid=True,
                    title='Cumulative Batting Average'
                ),
                showlegend=False
            )

            logger.info(f"Successfully generated plot for {player_name}")
            return fig.to_json()

        except Exception as e:
            logger.error(f"Error generating plot for {player_name}: {e}")
            return None