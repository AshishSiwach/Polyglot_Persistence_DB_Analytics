"""
Database Configuration Settings
Walmart UK Polyglot Persistence Project

IMPORTANT: Update these settings for your environment
For production, use environment variables via .env file
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file (if it exists)
load_dotenv()

# ============================================================================
# SQL SERVER CONFIGURATION
# ============================================================================
SQL_SERVER_CONFIG = {
    'server': os.getenv('SQL_SERVER', r'localhost\SQLEXPRESS'),  # Or your server name
    'database': os.getenv('SQL_DATABASE', 'WalmartUK'),
    'username': os.getenv('SQL_USER', 'sa'),  # Windows Auth: use your Windows username
    'password': os.getenv('SQL_PASSWORD', 'YourPassword123'),  # Set your password
    'driver': '{ODBC Driver 17 for SQL Server}'  # Or '{ODBC Driver 18 for SQL Server}'
}

# Connection string templates
SQL_CONNECTION_STRING = (
    f"DRIVER={SQL_SERVER_CONFIG['driver']};"
    f"SERVER={SQL_SERVER_CONFIG['server']};"
    f"DATABASE={SQL_SERVER_CONFIG['database']};"
    f"UID={SQL_SERVER_CONFIG['username']};"
    f"PWD={SQL_SERVER_CONFIG['password']}"
)

# For Windows Authentication, use this instead:
SQL_TRUSTED_CONNECTION_STRING = (
    f"DRIVER={SQL_SERVER_CONFIG['driver']};"
    f"SERVER={SQL_SERVER_CONFIG['server']};"
    f"DATABASE={SQL_SERVER_CONFIG['database']};"
    f"Trusted_Connection=yes;"
)

# ============================================================================
# MONGODB CONFIGURATION
# ============================================================================
MONGO_CONFIG = {
    'host': os.getenv('MONGO_HOST', 'localhost'),
    'port': int(os.getenv('MONGO_PORT', 27017)),
    'database': os.getenv('MONGO_DATABASE', 'walmart_uk'),
    'username': os.getenv('MONGO_USER', ''),  # Leave empty if no auth
    'password': os.getenv('MONGO_PASSWORD', '')  # Leave empty if no auth
}

# MongoDB connection string
if MONGO_CONFIG['username']:
    MONGO_CONNECTION_STRING = (
        f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@"
        f"{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{MONGO_CONFIG['database']}"
    )
else:
    MONGO_CONNECTION_STRING = (
        f"mongodb://{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
    )

# ============================================================================
# DATA PATHS
# ============================================================================
import os
from pathlib import Path

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'Data'

SQL_DATA_DIR = DATA_DIR / 'SQL_DATA_DIR'
MONGO_DATA_DIR = DATA_DIR / 'MONGO_DATA_DIR'

# CSV file paths for SQL Server
CSV_FILES = {
    'customers': SQL / 'customers.csv',
    'products': SQL_DATA_DIR / 'products.csv',
    'stores': SQL_DATA_DIR / 'stores.csv',
    'suppliers': SQL_DATA_DIR / 'suppliers.csv',
    'orders': SQL_DATA_DIR / 'orders.csv',
    'orderlines': SQL_DATA_DIR / 'orderlines.csv',
    'inventory': SQL_DATA_DIR / 'inventory.csv'
}

# JSON file paths for MongoDB
JSON_FILES = {
    'product_catalogue': MONGO_DATA_DIR / 'product_catalogue.json',
    'customer_reviews': MONGO_DATA_DIR / 'customer_reviews.json'
}

# ============================================================================
# USAGE INSTRUCTIONS
# ============================================================================
"""
SETUP INSTRUCTIONS:

1. Update SQL Server settings above (server, username, password)
2. Update MongoDB settings if not using localhost
3. For Windows Authentication SQL Server, use get_sql_connection(use_trusted=True)

OR create a .env file in project root:

SQL_SERVER=localhost
SQL_DATABASE=WalmartUK
SQL_USER=sa
SQL_PASSWORD=YourPassword123

MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DATABASE=walmart_uk
"""

if __name__ == "__main__":
    print("Configuration loaded successfully!")
    print(f"SQL Server: {SQL_SERVER_CONFIG['server']}/{SQL_SERVER_CONFIG['database']}")
    print(f"MongoDB: {MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{MONGO_CONFIG['database']}")
    print(f"Data directory: {DATA_DIR}")
