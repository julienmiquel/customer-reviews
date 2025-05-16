import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "bk_maps.log"

# Google Cloud settings
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
API_KEY = os.getenv('GOOGLE_API_KEY')

# BigQuery settings
BIGQUERY_DATASET_ID = 'burger_king_reviews_dataset'
BIGQUERY_TABLE_REVIEWS = 'france_reviews_v2'
BIGQUERY_TABLE_PLACE_DETAILS = 'places_details_v2'
BIGQUERY_DATASET_LOCATION = 'EU'

# Logging settings
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s" 