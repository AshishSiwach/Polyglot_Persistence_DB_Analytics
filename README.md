# Walmart UK Polyglot Persistence - Python Implementation

Complete Python implementation of the Walmart UK polyglot persistence system using **pyodbc** (SQL Server) and **pymongo** (MongoDB).

## 📋 Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Running Demonstrations](#running-demonstrations)
- [Video Recording Guide](#video-recording-guide)
- [Troubleshooting](#troubleshooting)

---

## Overview

This implementation demonstrates **polyglot persistence** - using SQL Server for structured transactional data and MongoDB for flexible unstructured data, linked via common identifiers (ProductID, CustomerID).

### What This Project Demonstrates

✅ **Time as Attribute** - OrderDate is an attribute, not a separate entity  
✅ **Order/OrderLine Structure** - Proper transaction model (not single Sales entity)  
✅ **Inventory Junction Table** - Explicit foreign keys (not "via inventory")  
✅ **Common Identifiers** - ProductID/CustomerID link both databases  
✅ **Cross-Database Queries** - Combining SQL sales + MongoDB sentiment  
✅ **Business Insights** - Only possible with polyglot persistence  

---

## Prerequisites

### Required Software

1. **Python 3.7+**
   ```bash
   python --version  # Should be 3.7 or higher
   ```

2. **SQL Server** (Express, Standard, or Enterprise)
   - Download: https://www.microsoft.com/en-us/sql-server/sql-server-downloads
   - Or use SQL Server LocalDB (lightweight option)

3. **MongoDB Community Server**
   - Download: https://www.mongodb.com/try/download/community
   - Or use MongoDB Atlas (cloud option)

4. **ODBC Driver for SQL Server**
   - Windows: Usually pre-installed
   - Mac: `brew install msodbcsql17`
   - Linux: Follow [Microsoft docs](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)

### Python Packages

```bash
pip install pyodbc pymongo pandas python-dotenv faker
```

Or install from requirements.txt:
```bash
pip install -r 01_Setup/requirements.txt
```

---

## Project Structure

```
Python_Implementation/
│
├── 01_Setup/
│   ├── requirements.txt          # Python dependencies
│   └── config.py                 # Database connection settings
│
├── 02_Data_Generation/
│   └── generate_walmart_data.py  # Generate UK retail sample data
│
├── 03_Data/                      # Generated data (created by script)
│   ├── SQL_Server/               # CSV files
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   ├── stores.csv
│   │   ├── suppliers.csv
│   │   ├── orders.csv
│   │   ├── orderlines.csv
│   │   └── inventory.csv
│   │
│   └── MongoDB/                  # JSON files
│       ├── product_catalogue.json
│       └── customer_reviews.json
│
├── 04_SQL_Server/
│   ├── sql_utils.py              # SQL Server helper functions
│   ├── 01_create_tables.py      # Create 7 tables
│   ├── 02_import_data.py        # Import CSV data
│   └── 03_demo_queries.py       # Demonstration queries
│
├── 05_MongoDB/
│   ├── mongo_utils.py            # MongoDB helper functions
│   ├── 01_create_collections.py # Create collections with validation
│   ├── 02_import_data.py        # Import JSON data
│   ├── 03_create_indexes.py    # Create indexes on common identifiers
│   └── 04_demo_queries.py       # Demonstration queries
│
├── 06_Polyglot_Demo/
│   └── cross_database_demo.py   # THE CRITICAL PROOF - combines both DBs
│
├── main.py                       # Master orchestrator (run this!)
└── README.md                     # This file
```

---

## Quick Start

### 1. Configure Database Connections

Edit `01_Setup/config.py`:

```python
SQL_SERVER_CONFIG = {
    'server': 'localhost',          # Your SQL Server name
    'database': 'WalmartUK',
    'username': 'sa',               # Your SQL username
    'password': 'YourPassword123',  # Your SQL password
    'driver': '{ODBC Driver 17 for SQL Server}'
}

MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'walmart_uk'
}
```

**For Windows Authentication (SQL Server):**
```python
# In sql_utils.py, use:
conn = get_sql_connection(use_trusted=True)
```

### 2. Run Complete Setup

```bash
python main.py
```

This runs all steps automatically:
- ✅ Generates sample data
- ✅ Creates SQL Server tables
- ✅ Imports SQL data
- ✅ Creates MongoDB collections
- ✅ Imports MongoDB data
- ✅ Runs demonstrations

**Expected duration:** 2-5 minutes

---

## Detailed Setup

### Option 1: Run Individual Steps (Recommended for Video)

```bash
# Step 1: Generate Data
python 02_Data_Generation/generate_walmart_data.py

# Step 2: SQL Server Setup
python 04_SQL_Server/01_create_tables.py
python 04_SQL_Server/02_import_data.py
python 04_SQL_Server/03_demo_queries.py

# Step 3: MongoDB Setup
python 05_MongoDB/01_create_collections.py
python 05_MongoDB/02_import_data.py
python 05_MongoDB/03_create_indexes.py
python 05_MongoDB/04_demo_queries.py

# Step 4: Polyglot Persistence Proof
python 06_Polyglot_Demo/cross_database_demo.py
```

### Option 2: Custom Data Amounts

Edit `02_Data_Generation/generate_walmart_data.py`:

```python
NUM_CUSTOMERS = 5000   # Change from 1000
NUM_ORDERS = 2000      # Change from 500
NUM_REVIEWS = 1000     # Change from 300
```

Then regenerate data.

---

## Running Demonstrations

### SQL Server Demonstrations

```bash
python 04_SQL_Server/03_demo_queries.py
```

**Shows:**
- ✅ Time-based analysis (OrderDate attribute)
- ✅ Market basket analysis (Order/OrderLine structure)
- ✅ Inventory junction table (explicit FKs)
- ✅ Customer lifetime value (complex joins)

### MongoDB Demonstrations

```bash
python 05_MongoDB/04_demo_queries.py
```

**Shows:**
- ✅ Schema flexibility (different attributes per product)
- ✅ Sentiment analysis (aggregation pipeline)
- ✅ Full-text search (text index)
- ✅ Common identifiers (ProductID verification)

### Cross-Database Demo (THE PROOF!)

```bash
python 06_Polyglot_Demo/cross_database_demo.py
```

**Proves:**
- ✅ ProductID exists in BOTH databases
- ✅ SQL sales data + MongoDB sentiment = combined insights
- ✅ Business value from polyglot persistence

**Example Output:**
```
ProductID  Name                Revenue      Rating  Sentiment
5          Samsung TV          £20,677.00   4.25⭐   75.0%
1          Angus Steak         £1,844.58    4.67⭐   88.9%

⚠ HIGH SALES BUT LOW SATISFACTION:
  • Samsung TV: £20,677 revenue but 4.25⭐ rating
  → Action: Investigate quality issues
```

---

## Video Recording Guide

### Recording Individual Demos

For your video presentation, run scripts individually and narrate:

#### SQL Server Demo (~3 minutes)

```bash
python 04_SQL_Server/03_demo_queries.py
```

**What to say:**
1. "I'll start in SQL Server. Notice OrderDate is an attribute..."
2. [Execute query, show results]
3. "This query shows products bought together - only possible with Order/OrderLine structure..."
4. [Execute query, show results]
5. "Inventory has three foreign keys: ProductID, StoreID, SupplierID..."

#### MongoDB Demo (~2 minutes)

```bash
python 05_MongoDB/04_demo_queries.py
```

**What to say:**
1. "Switching to MongoDB. See how ProductID appears here - our common identifier..."
2. [Execute query, show results]
3. "Different products have different attributes - natural flexibility..."

#### Polyglot Demo (~3 minutes)

```bash
python 06_Polyglot_Demo/cross_database_demo.py
```

**What to say:**
1. "This is the critical proof - ProductID exists in BOTH databases..."
2. [Show verification results]
3. "Now combining SQL sales with MongoDB sentiment..."
4. [Show combined analysis]
5. "This insight - high sales but low satisfaction - is only possible with polyglot persistence!"

### Screen Recording Tips

✅ **DO:**
- Set terminal/IDE to 1920x1080 resolution
- Increase font size (16-18pt minimum)
- Show query execution, not just code
- Point to specific results with cursor
- Narrate WHILE executing, not after

❌ **DON'T:**
- Just scroll through code
- Show only the first few rows (show ALL results)
- Talk about something different than what's on screen
- Skip the cross-database demo

---

## Troubleshooting

### SQL Server Connection Issues

**Error:** `Login failed for user 'sa'`

**Solution:**
```python
# In config.py, use Windows Authentication:
conn = get_sql_connection(use_trusted=True)
```

**Error:** `[ODBC Driver 17 for SQL Server] not found`

**Solution:**
```bash
# Check available drivers:
import pyodbc
print(pyodbc.drivers())

# Update config.py with available driver:
'driver': '{ODBC Driver 18 for SQL Server}'
```

### MongoDB Connection Issues

**Error:** `ServerSelectionTimeoutError`

**Solution:**
```bash
# Check if MongoDB is running:
# Windows:
net start MongoDB

# Mac:
brew services start mongodb-community

# Linux:
sudo systemctl start mongod
```

### Data Import Issues

**Error:** `CSV file not found`

**Solution:**
```bash
# Ensure you ran data generation first:
python 02_Data_Generation/generate_walmart_data.py

# Check 03_Data/ directory exists with CSV/JSON files
```

**Error:** `Row count mismatch`

**Solution:**
- Delete and regenerate data
- Clear SQL Server tables: Drop and recreate database
- Clear MongoDB collections: `db.dropDatabase()`

### BULK INSERT Permission Denied

**Error:** `Cannot bulk load because the file could not be opened`

**Solution:**
```python
# Option 1: Grant SQL Server service account permissions to CSV folder

# Option 2: Use row-by-row insert (slower but works):
# In 02_import_data.py, the script already uses this method
```

---

## Common Questions

### Q: Can I use this with existing data?

**A:** Yes! Modify the import scripts to read from your CSV/JSON files instead.

### Q: How do I reset everything?

**A:** Run these commands:

```sql
-- SQL Server (in SSMS)
DROP DATABASE WalmartUK;
```

```javascript
// MongoDB (in mongosh)
use walmart_uk
db.dropDatabase()
```

Then re-run `python main.py`

### Q: Can I use PostgreSQL instead of SQL Server?

**A:** Yes, but you'll need to:
1. Change `import pyodbc` to `import psycopg2`
2. Update SQL syntax (e.g., `IDENTITY` → `SERIAL`)
3. Modify connection string in config.py

### Q: Where's the PowerBI file?

**A:** PowerBI files (.pbix) aren't included. Create your own:
1. Get Data → SQL Server → Connect to WalmartUK
2. Get Data → MongoDB → Connect to walmart_uk
3. Model View → Create relationships on ProductID

---

## What to Include in Your Report

### Evidence of Execution

✅ **Screenshots showing:**
- Query results (not just code)
- Row counts matching expected values
- Cross-database demo output proving common identifiers work

✅ **Video demonstration showing:**
- Live query execution (not screenshots)
- Results appearing on screen
- Narration synchronized with execution

### Key Points to Emphasize

1. **"Time is not a separate entity"**
   - Show OrderDate as attribute in Order table
   - Show time-based queries working without Time entity

2. **"Sales is not a single entity"**
   - Show Order + OrderLine structure
   - Show market basket analysis (impossible with single Sales table)

3. **"Relationships not labeled 'via inventory'"**
   - Show Inventory table with 3 explicit foreign keys
   - Show junction table enabling M:M relationships

4. **"Common identifiers"**
   - Show ProductID in SQL Server
   - Show ProductID in MongoDB
   - Show cross-database query combining both

---

## Success Criteria

✅ All 7 SQL Server tables created  
✅ 1,000 customers imported  
✅ 500 orders with ~1,865 order lines imported  
✅ 2 MongoDB collections created  
✅ 41 products in both SQL and MongoDB  
✅ 300 reviews imported  
✅ ProductID matches in both databases (verified)  
✅ Cross-database query returns combined results  

If all checks pass, your polyglot persistence system is working correctly!

---

## Support

For issues:
1. Check Troubleshooting section above
2. Review error messages carefully
3. Verify database connections in config.py
4. Ensure both SQL Server and MongoDB are running

---

## License

Educational project for BEMM459 Database Technologies module.

---

**Good luck with your demonstration! 🚀📊**

Remember: The key is **SHOWING IT WORKING**, not just showing code!
