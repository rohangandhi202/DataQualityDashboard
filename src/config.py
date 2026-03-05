import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# PostgreSQL Connection Settings
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'data_quality_demo')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')

# Construct connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Great Expectations Settings
GE_PROJECT_DIR = 'great_expectations'
GE_DATA_SOURCE = 'postgres'

# Data paths
DATA_DIR = '../data'
SAMPLE_DATA_PATH = os.path.join(DATA_DIR, 'sample_transactions.csv')

# Validation Settings
VALIDATION_BATCH_SIZE = 1000
ALERT_THRESHOLD = 0.9  # Alert if quality score drops below 90%