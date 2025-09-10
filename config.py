from dotenv import load_dotenv
load_dotenv()

import os
from datetime import datetime
from urllib.parse import urlparse

# Database configuration
# Railway provides MYSQL_URL in the format: mysql://username:password@host:port/database
mysql_url = os.getenv("MYSQL_URL")
db_url = os.getenv("DATABASE_URL") or mysql_url  # Try both common Railway env vars

if db_url:
    try:
        url = urlparse(db_url)
        DB_CONFIG = {
            'host': url.hostname,
            'user': url.username,
            'password': url.password,
            'database': url.path[1:] if url.path and len(url.path) > 1 else 'railway',  # Default to 'railway' if no database in URL
            'port': url.port or 3306  # Default MySQL port
        }
        print(f"Using Railway MySQL connection to host: {url.hostname}")
    except Exception as e:
        print(f"Error parsing database URL: {e}")
        # Fallback configuration
        DB_CONFIG = {
            'host': os.getenv("MYSQL_HOST", "localhost"),
            'user': os.getenv("MYSQL_USER", "root"),
            'password': os.getenv("MYSQL_PASSWORD", ""),
            'database': os.getenv("MYSQL_DATABASE", "railway"),
            'port': int(os.getenv("MYSQL_PORT", 3306))
        }
else:
    # Fallback for local development or if no URL is provided
    DB_CONFIG = {
        'host': os.getenv("MYSQL_HOST", os.getenv("DB_HOST", "localhost")),
        'user': os.getenv("MYSQL_USER", os.getenv("DB_USER", "root")),
        'password': os.getenv("MYSQL_PASSWORD", os.getenv("DB_PASSWORD", "")),
        'database': os.getenv("MYSQL_DATABASE", os.getenv("DB_NAME", "railway")),
        'port': int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", 3306)))
    }
    print("Using fallback MySQL configuration")

# Debug print (remove in production)
print(f"Database config: host={DB_CONFIG['host']}, user={DB_CONFIG['user']}, database={DB_CONFIG['database']}, port={DB_CONFIG['port']}")

# Team mapping
TEAM_MAPPING = {
    'IND': 'India',
    'PAK': 'Pakistan',
    'AUS': 'Australia',
    'ENG': 'England',
    'BAN': 'Bangladesh',
    'AFG': 'Afghanistan',
    'IRE': 'Ireland',
    'SA': 'South Africa',
    'SL': 'Sri Lanka',
    'NZ': 'New Zealand',
    'WI': 'West Indies',
    'ZIM': 'Zimbabwe'
}

# Dataset configurations
DATASET_CONFIGS = {
    'team': {
        'min_columns': 11,
        'columns': ["Team", "ScoreDescending", "Overs", "RPO", "Lead", "Inns",
                   "Result", "", "Opposition", "Ground", "Start Date"]
    },
    'batting': {
        'min_columns': 12,
        'columns': ["Player", "RunsDescending", "Mins", "BF", "4s", "6s", "SR",
                   "Inns", "", "Opposition", "Ground", "Start Date"]
    },
    'bowling': {
        'min_columns': 12,
        'columns': ["Player", "Overs", "Mdns", "Runs", "WktsDescending", "Econ",
                   "Inns", "", "Opposition", "Ground", "Start Date"]
    }
}

# URL templates
URL_TEMPLATES = {
    'team': "/ci/engine/stats/index.html?class=1;home_or_away=1;home_or_away=2;home_or_away=3;result=1;result=2;result=3;result=4;spanmin1={start_date};spanmax1=13+Aug+2050;spanval1=span;template=results;type=team;view=innings",
    'batting': "/ci/engine/stats/index.html?class=1;home_or_away=1;home_or_away=2;home_or_away=3;result=1;result=2;result=3;result=4;spanmin1={start_date};spanmax1=13+Aug+2050;spanval1=span;template=results;type=batting;view=innings",
    'bowling': "/ci/engine/stats/index.html?class=1;home_or_away=1;home_or_away=2;home_or_away=3;result=1;result=2;result=3;result=4;spanmin1={start_date};spanmax1=13+Aug+2050;spanval1=span;template=results;type=bowling;view=innings"
}

BASE_URL = "https://stats.espncricinfo.com"

# Chrome options for WebDriver
CHROME_OPTIONS = [
    '--headless',
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--disable-web-security',
    '--allow-running-insecure-content',
    '--disable-extensions',
    '--disable-plugins',
    '--disable-default-apps',
    '--disable-sync',
    '--disable-translate',
    '--disable-background-timer-throttling',
    '--disable-backgrounding-occluded-windows',
    '--disable-renderer-backgrounding',
    '--disable-features=TranslateUI',
    '--disable-ipc-flooding-protection',
    '--homedir=/tmp'
]

CHROME_BINARY_LOCATION = '/usr/bin/google-chrome'