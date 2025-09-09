# Cricket Analytics System

A comprehensive cricket statistics platform that scrapes, processes, and analyzes cricket data from ESPN Cricinfo. The system provides detailed analytics on batting and bowling performance, team outcomes, and country-wise statistics.

## Features

### Data Collection
- Automated web scraping from ESPN Cricinfo
- Support for team, batting, and bowling statistics
- Incremental data updates based on latest database records
- Selenium-based scraping with pagination handling

### Analytics Engine
- Player performance analysis across different countries
- Batsman vs bowler head-to-head comparisons
- Team match outcome correlations with player performance
- Cumulative batting and bowling average visualizations
- Performance trends over time

### Automated Workflows
- Apache Airflow integration for scheduled data scraping
- Weekly automated data updates (Mondays at 2 AM)
- Health checks and notification systems
- Parallel processing of different data types

### API Endpoints
- RESTful API for data access and analysis
- Real-time plot generation with Plotly
- Player search and statistics retrieval
- Comprehensive error handling and logging

## System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web Scraper   │    │  Data Processor  │    │    Database     │
│  (Selenium)     │────▶│   (Pandas)      │────▶│    (MySQL)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Flask API      │    │ Analytics Engine │    │   Airflow DAGs  │
│  (REST API)     │◀───│   (Statistics)   │    │  (Scheduling)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available for containers
- Internet connection for web scraping

## Quick Start

### 1. Clone and Setup
```bash
git clone <repository-url>
cd cricket-analytics
```

### 2. Configure Environment
Update database credentials in `config.py` or use environment variables:
```bash
export DB_HOST=mysql
export DB_USER=cricket_user
export DB_PASSWORD=your_secure_password
export DB_NAME=cricket_stats
```

### 3. Start Services
```bash
docker-compose up -d
```

This will start:
- MySQL database (port 3307)
- Flask API (port 5000)
- Airflow webserver (port 8080)
- Airflow scheduler
- PostgreSQL (for Airflow metadata)

### 4. Initialize Database
The database tables will be created automatically when the first scraping operation runs.

### 5. Access Applications
- **API**: http://localhost:5000
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Health Check**: http://localhost:5000/health

## API Usage

### Data Scraping
```bash
# Scrape team data
curl "http://localhost:5000/scrape/team"

# Scrape batting data
curl "http://localhost:5000/scrape/batting"

# Scrape bowling data
curl "http://localhost:5000/scrape/bowling"
```

### Player Analytics
```bash
# Get batting players list
curl "http://localhost:5000/api/players"

# Get bowling players list
curl "http://localhost:5000/api/bowling-players"

# Player batting performance by country
curl "http://localhost:5000/analysis/batting-by-country?player=Virat Kohli"

# Player bowling performance by country
curl "http://localhost:5000/analysis/bowling-by-country?player=Jasprit Bumrah"
```

### Performance Analysis
```bash
# Batsman vs bowler analysis
curl "http://localhost:5000/analysis/batsman-vs-bowler?batsman=Virat Kohli&bowler=James Anderson"

# Team outcomes when player scores 50+ runs
curl "http://localhost:5000/analysis/batting-outcomes?player=Virat Kohli&min_score=50"

# Team outcomes when bowler takes 3+ wickets
curl "http://localhost:5000/analysis/bowling-outcomes?player=Jasprit Bumrah&min_wickets=3"
```

### Visualizations
```bash
# Batting average plot over time
curl "http://localhost:5000/plot/batting?player=Virat Kohli"

# Bowling performance plot over time
curl "http://localhost:5000/plot/bowling?player=Jasprit Bumrah"
```

## Airflow Workflows

### Weekly Scraping DAG
- **Schedule**: Every Monday at 2:00 AM
- **Tasks**: Health check → Parallel scraping (team, batting, bowling) → Notification
- **Retries**: 2 attempts with 5-minute delays
- **Monitoring**: Automatic failure notifications

### Manual Triggers
Access the Airflow UI at http://localhost:8080 to manually trigger DAGs or monitor execution status.

## Database Schema

### Team Table
- Team, Opposition, Ground, Start Date
- Score, Wickets, Overs, RPO, Result
- Match outcome and venue information

### Batting Table
- Player, Team, Opposition, Ground, Start Date
- Runs, Balls Faced, Strike Rate, 4s, 6s
- Not out status and innings number

### Bowling Table
- Player, Team, Opposition, Ground, Start Date
- Overs, Maidens, Runs, Wickets, Economy
- Bowling figures and match details

## Configuration

### Environment Variables
```bash
# Database Configuration
DB_HOST=mysql
DB_USER=cricket_user
DB_PASSWORD=your_password
DB_NAME=cricket_stats

# Chrome Settings (for scraping)
CHROME_BINARY_LOCATION=/usr/bin/google-chrome
```

### Scraping Parameters
- **Date Range**: From August 13, 2022 onwards
- **Update Strategy**: Incremental based on latest database records
- **Timeout Settings**: 5-minute timeout per request
- **Retry Logic**: 2 retries with exponential backoff

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   ```bash
   # Check MySQL container status
   docker-compose logs mysql
   
   # Verify connection
   docker-compose exec mysql mysql -u cricket_user -p cricket_stats
   ```

2. **Scraping Failures**
   ```bash
   # Check Chrome/Selenium setup
   docker-compose logs app
   
   # Verify ESPN Cricinfo accessibility
   curl -I https://stats.espncricinfo.com
   ```

3. **Airflow Issues**
   ```bash
   # Check Airflow logs
   docker-compose logs airflow-webserver
   docker-compose logs airflow-scheduler
   
   # Reset Airflow database
   docker-compose exec airflow-webserver airflow db reset
   ```

### Performance Optimization

1. **Memory Usage**
   - Monitor container memory with `docker stats`
   - Adjust `--shm-size` for Chrome if needed

2. **Scraping Speed**
   - Implement request delays to avoid rate limiting
   - Use headless Chrome for better performance

3. **Database Performance**
   - Add indexes on frequently queried columns
   - Regular cleanup of old temporary data

## Development

### Local Development Setup
```bash
# Install Python dependencies
pip install -r requirements.txt

# Run Flask app locally
python app.py

# Run individual components
python cricket_service.py
python analytics_service.py
```

### Adding New Analytics
1. Add method to `AnalyticsService` class
2. Create corresponding Flask route in `app.py`
3. Update API documentation
4. Add tests for new functionality

### Custom DAGs
Place custom Airflow DAGs in the `dags/` directory. They will be automatically loaded by the scheduler.

## Security Considerations

- Change default database passwords
- Use environment variables for sensitive data
- Implement API authentication for production
- Regular security updates for base Docker images
- Monitor for unusual scraping patterns

## Support and Contributing

### Logging
- Application logs: Check Docker container logs
- Airflow logs: Available in the Airflow UI
- Database logs: MySQL container logs

### Monitoring
- Health check endpoint: `/health`
- Database record counts in logs
- Airflow task success/failure tracking

---

**Note**: Remember to update database passwords and other sensitive configurations before deploying to production.#   C r i c k e t - D a t a - S c r a p i n g - a n d - A n a l y t i c s  
 