import logging
from flask import Flask, render_template, jsonify, request
import pandas as pd
import json

# Initialize logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)
logger = logging.getLogger(__name__)

# Initialize services with error handling
cricket_service = None
db_manager = None
analytics_service = None

try:
    from cricket_service import CricketService
    from database import DatabaseManager  
    from analytics_service import AnalyticsService
    
    cricket_service = CricketService()
    db_manager = DatabaseManager()
    analytics_service = AnalyticsService(db_manager)
    logger.info("All services initialized successfully")
    
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error("Some modules are missing. Check if all files are in the correct location.")
except Exception as e:
    logger.error(f"Error initializing services: {e}")
    logger.error("Services will be unavailable, but basic Flask routes will work")

@app.route('/')
def home():
    """Home page - works even without template"""
    try:
        return render_template('index.html')
    except:
        # Fallback if template is missing
        return jsonify({
            "message": "Cricket Analytics API is running",
            "status": "OK",
            "available_endpoints": [
                "/scrape/<dataset_type>",
                "/api/players", 
                "/api/bowling-players",
                "/plot/batting?player=<name>",
                "/plot/bowling?player=<name>",
                "/analysis/batsman-vs-bowler?batsman=<name>&bowler=<name>",
                "/analysis/batting-outcomes?player=<name>&min_score=<score>",
                "/analysis/bowling-outcomes?player=<name>&min_wickets=<wickets>",
                "/analysis/batting-by-country?player=<name>",
                "/analysis/bowling-by-country?player=<name>"
            ]
        })

@app.route('/health')
def health_check():
    """Health check endpoint"""
    services_status = {
        "cricket_service": cricket_service is not None,
        "db_manager": db_manager is not None,
        "analytics_service": analytics_service is not None
    }
    
    return jsonify({
        "status": "OK",
        "services": services_status,
        "message": "App is running"
    })

@app.route('/scrape/<dataset_type>', methods=['GET'])
def scrape(dataset_type):
    """Scrape and process cricket data for the specified dataset type"""
    if cricket_service is None:
        return jsonify({"error": "Cricket service not available. Check logs for initialization errors."}), 503
    
    try:
        logger.info(f"Scraping dataset: {dataset_type}")
        
        result_df = cricket_service.scrape_and_process_data(dataset_type)
        
        return jsonify({
            "message": f"Data scraped and processed successfully for {dataset_type}!",
            "rows_processed": len(result_df),
            "data": result_df.to_dict(orient='records') if result_df is not None else []
        })
    
    except ValueError as e:
        logger.error(f"Invalid dataset type: {e}")
        return jsonify({"error": str(e)}), 400
    
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        return jsonify({"error": f"Scraping failed: {str(e)}"}), 500

@app.route('/api/players', methods=['GET'])
def get_players():
    """Fetch all unique player names from the batting table."""
    if db_manager is None:
        return jsonify({"error": "Database manager not available"}), 503
        
    try:
        players = db_manager.fetch_unique_batting_players()
        return jsonify(players)
    except Exception as e:
        return jsonify({"error": f"Failed to fetch batting players: {str(e)}"}), 500

@app.route('/api/bowling-players', methods=['GET'])
def get_bowling_players():
    """Fetch all unique player names from the bowling table."""
    if db_manager is None:
        return jsonify({"error": "Database manager not available"}), 503
        
    try:
        players = db_manager.fetch_unique_bowling_players()
        return jsonify(players)
    except Exception as e:
        return jsonify({"error": f"Failed to fetch bowling players: {str(e)}"}), 500

@app.route('/plot/batting', methods=['GET'])
def get_batting_plot():
    """Generate and return a cumulative batting average plot for a specific player."""
    if analytics_service is None:
        return jsonify({"error": "Analytics service not available"}), 503
        
    try:
        player_name = request.args.get('player')
        if not player_name:
            return jsonify({"error": "No player name provided. Please select a player."}), 400
        
        plot_json = analytics_service.generate_player_batting_average_plot(player_name)
        
        if plot_json is None:
            return jsonify({"error": f"No data found for player: {player_name}. Please scrape batting data first."}), 404
        
        plot_dict = json.loads(plot_json)
        return jsonify(plot_dict), 200
        
    except Exception as e:
        logger.error(f"Failed to generate batting plot: {str(e)}")
        return jsonify({"error": f"Failed to generate batting plot: {str(e)}"}), 500

@app.route('/plot/bowling', methods=['GET'])
def get_bowling_plot():
    """Generate and return a bowling performance plot for a specific player."""
    if analytics_service is None:
        return jsonify({"error": "Analytics service not available"}), 503
        
    try:
        player_name = request.args.get('player')
        if not player_name:
            return jsonify({"error": "No player name provided. Please select a player."}), 400
        
        plot_json = analytics_service.generate_player_bowling_average_plot(player_name)
        
        if plot_json is None:
            return jsonify({"error": f"No bowling data found for player: {player_name}. Please scrape bowling data first."}), 404
        
        plot_dict = json.loads(plot_json)
        return jsonify(plot_dict), 200
        
    except Exception as e:
        logger.error(f"Failed to generate bowling plot: {str(e)}")
        return jsonify({"error": f"Failed to generate bowling plot: {str(e)}"}), 500

@app.route('/analysis/batsman-vs-bowler', methods=['GET'])
def get_batsman_vs_bowler_analysis():
    """Analyze batsman's performance against a specific bowler or opposition team."""
    if analytics_service is None:
        return jsonify({"error": "Analytics service not available"}), 503
        
    try:
        batsman_name = request.args.get('batsman')
        bowler_name = request.args.get('bowler')
        
        if not batsman_name:
            return jsonify({"error": "No batsman name provided. Please specify a batsman."}), 400
        
        analysis_result = analytics_service.analyze_batsman_vs_bowler(batsman_name, bowler_name)
        
        if analysis_result is None:
            return jsonify({"error": f"Failed to analyze performance for batsman: {batsman_name}"}), 500
        
        if 'error' in analysis_result:
            return jsonify(analysis_result), 404
        
        return jsonify(analysis_result), 200
        
    except Exception as e:
        logger.error(f"Failed to analyze batsman vs bowler: {str(e)}")
        return jsonify({"error": f"Failed to analyze batsman vs bowler: {str(e)}"}), 500

@app.route('/analysis/batting-outcomes', methods=['GET'])
def get_batting_match_outcomes():
    """Analyze team match outcomes when a player scores above a minimum threshold."""
    if analytics_service is None:
        return jsonify({"error": "Analytics service not available"}), 503
        
    try:
        player_name = request.args.get('player')
        min_score = request.args.get('min_score')
        
        if not player_name:
            return jsonify({"error": "No player name provided. Please specify a player."}), 400
        
        if not min_score:
            return jsonify({"error": "No minimum score provided. Please specify min_score."}), 400
        
        try:
            min_score = int(min_score)
        except ValueError:
            return jsonify({"error": "Invalid minimum score. Please provide a valid integer."}), 400
        
        if min_score < 0:
            return jsonify({"error": "Minimum score must be non-negative."}), 400
        
        analysis_result = analytics_service.analyze_batting_match_outcomes(player_name, min_score)
        
        if analysis_result is None:
            return jsonify({"error": f"Failed to analyze batting outcomes for player: {player_name}"}), 500
        
        if 'error' in analysis_result:
            return jsonify(analysis_result), 404
        
        return jsonify(analysis_result), 200
        
    except Exception as e:
        logger.error(f"Failed to analyze batting match outcomes: {str(e)}")
        return jsonify({"error": f"Failed to analyze batting match outcomes: {str(e)}"}), 500

@app.route('/analysis/bowling-outcomes', methods=['GET'])
def get_bowling_match_outcomes():
    """Analyze team match outcomes when a player takes above a minimum wickets threshold."""
    if analytics_service is None:
        return jsonify({"error": "Analytics service not available"}), 503
        
    try:
        player_name = request.args.get('player')
        min_wickets = request.args.get('min_wickets')
        
        if not player_name:
            return jsonify({"error": "No player name provided. Please specify a player."}), 400
        
        if not min_wickets:
            return jsonify({"error": "No minimum wickets provided. Please specify min_wickets."}), 400
        
        try:
            min_wickets = int(min_wickets)
        except ValueError:
            return jsonify({"error": "Invalid minimum wickets. Please provide a valid integer."}), 400
        
        if min_wickets < 0:
            return jsonify({"error": "Minimum wickets must be non-negative."}), 400
        
        analysis_result = analytics_service.analyze_bowling_match_outcomes(player_name, min_wickets)
        
        if analysis_result is None:
            return jsonify({"error": f"Failed to analyze bowling outcomes for player: {player_name}"}), 500
        
        if 'error' in analysis_result:
            return jsonify(analysis_result), 404
        
        return jsonify(analysis_result), 200
        
    except Exception as e:
        logger.error(f"Failed to analyze bowling match outcomes: {str(e)}")
        return jsonify({"error": f"Failed to analyze bowling match outcomes: {str(e)}"}), 500

@app.route('/analysis/batting-by-country', methods=['GET'])
def get_batting_by_country():
    """Analyze a player's batting performance by country."""
    if analytics_service is None:
        return jsonify({"error": "Analytics service not available"}), 503
        
    try:
        player_name = request.args.get('player')
        
        if not player_name:
            return jsonify({"error": "No player name provided. Please specify a player."}), 400
        
        analysis_result = analytics_service.analyze_batting_by_country(player_name)
        
        if analysis_result is None:
            return jsonify({"error": f"Failed to analyze batting by country for player: {player_name}"}), 500
        
        if 'error' in analysis_result:
            return jsonify(analysis_result), 404
        
        return jsonify(analysis_result), 200
        
    except Exception as e:
        logger.error(f"Failed to analyze batting by country: {str(e)}")
        return jsonify({"error": f"Failed to analyze batting by country: {str(e)}"}), 500

@app.route('/analysis/bowling-by-country', methods=['GET'])
def get_bowling_by_country():
    """Analyze a player's bowling performance by country."""
    if analytics_service is None:
        return jsonify({"error": "Analytics service not available"}), 503
        
    try:
        player_name = request.args.get('player')
        
        if not player_name:
            return jsonify({"error": "No player name provided. Please specify a player."}), 400
        
        analysis_result = analytics_service.analyze_bowling_by_country(player_name)
        
        if analysis_result is None:
            return jsonify({"error": f"Failed to analyze bowling by country for player: {player_name}"}), 500
        
        if 'error' in analysis_result:
            return jsonify(analysis_result), 404
        
        return jsonify(analysis_result), 200
        
    except Exception as e:
        logger.error(f"Failed to analyze bowling by country: {str(e)}")
        return jsonify({"error": f"Failed to analyze bowling by country: {str(e)}"}), 500

if __name__ == '__main__':
    print("Starting Cricket Analytics API...")
    print("Access at: http://localhost:5000")
    print("Health check: http://localhost:5000/health")
    app.run(debug=True, host='0.0.0.0', port=5000)