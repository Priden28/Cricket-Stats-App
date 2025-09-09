from dotenv import load_dotenv
load_dotenv()

import os
from datetime import datetime

# Database configuration
DB_CONFIG = {
    'host': os.getenv("DB_HOST", "mysql"),
    'user': os.getenv("DB_USER", "cricket_user"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME", "cricket_stats")
}

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