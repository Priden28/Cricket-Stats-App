from dotenv import load_dotenv
load_dotenv()

import os
from datetime import datetime
from urllib.parse import urlparse

# Database configuration
# Railway provides MYSQL_URL in the format: mysql://username:password@host:port/database
mysql_url = os.getenv("MYSQL_URL")

if mysql_url:
    try:
        url = urlparse(mysql_url)
        DB_CONFIG = {
            'host': url.hostname,
            'user': url.username,
            'password': url.password,
            'database': url.path[1:] if url.path and len(url.path) > 1 else 'railway',
            'port': url.port or 3306,
            'ssl_disabled': True,  # Railway MySQL doesn't require SSL
            'autocommit': True
        }
        print(f"✅ Using Railway MySQL: {url.hostname}:{url.port}/{url.path[1:] if url.path else 'railway'}")
    except Exception as e:
        print(f"❌ Error parsing MYSQL_URL: {e}")
        print(f"Raw MYSQL_URL: {mysql_url}")
        raise Exception(f"Failed to parse Railway database URL: {e}")
else:
    # Fallback for local development
    print("⚠️  No MYSQL_URL found, using local development config")
    DB_CONFIG = {
        'host': os.getenv("DB_HOST", "localhost"),
        'user': os.getenv("DB_USER", "root"),
        'password': os.getenv("DB_PASSWORD", ""),
        'database': os.getenv("DB_NAME", "cricket_stats"),
        'port': int(os.getenv("DB_PORT", 3306)),
        'autocommit': True
    }

# Debug output
print(f"🔧 Final DB Config: host={DB_CONFIG['host']}, user={DB_CONFIG['user']}, database={DB_CONFIG['database']}, port={DB_CONFIG['port']}")
print(f"🌍 Environment: {'PRODUCTION (Railway)' if mysql_url else 'DEVELOPMENT (Local)'}")

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