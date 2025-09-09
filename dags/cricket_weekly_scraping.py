from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'cricket-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'cricket_weekly_scraping',
    default_args=default_args,
    description='Weekly cricket statistics scraping from ESPN Cricinfo',
    schedule_interval='0 2 * * 1',  # Every Monday at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['cricket', 'scraping', 'weekly']
)

def scrape_cricket_data(dataset_type):
    """
    Function to call your Flask scraping endpoint
    """
    try:
        # Assuming your cricket-scraper service is accessible
        url = f"http://cricket-scraper:5000/scrape/{dataset_type}"
        response = requests.get(url, timeout=300)  # 5 minute timeout
        
        if response.status_code == 200:
            logging.info(f"Successfully scraped {dataset_type} data")
            return response.json()
        else:
            logging.error(f"Failed to scrape {dataset_type}: {response.status_code}")
            raise Exception(f"Scraping failed with status {response.status_code}")
    
    except requests.exceptions.Timeout:
        logging.error(f"Timeout while scraping {dataset_type}")
        raise
    except Exception as e:
        logging.error(f"Error scraping {dataset_type}: {str(e)}")
        raise

# Task to scrape team data
scrape_team_data = PythonOperator(
    task_id='scrape_team_data',
    python_callable=lambda: scrape_cricket_data('team'),
    dag=dag
)

# Task to scrape batting data
scrape_batting_data = PythonOperator(
    task_id='scrape_batting_data',
    python_callable=lambda: scrape_cricket_data('batting'),
    dag=dag
)

# Task to scrape bowling data
scrape_bowling_data = PythonOperator(
    task_id='scrape_bowling_data',
    python_callable=lambda: scrape_cricket_data('bowling'),
    dag=dag
)

# Optional: Add a completion notification task
def send_completion_notification(**context):
    """
    Send notification when all scraping is complete
    """
    logging.info("Weekly cricket data scraping completed successfully!")
    # You can add email/Slack notifications here
    
completion_notification = PythonOperator(
    task_id='completion_notification',
    python_callable=send_completion_notification,
    dag=dag
)

# Optional: Health check task to verify services are running
health_check = SimpleHttpOperator(
    task_id='health_check',
    http_conn_id='default_http',  # Use default connection
    endpoint='http://app:5000/',
    method='GET',
    dag=dag
)

# Define task dependencies
# Run health check first, then scraping tasks in parallel, then notification
health_check >> [scrape_team_data, scrape_batting_data, scrape_bowling_data] >> completion_notification