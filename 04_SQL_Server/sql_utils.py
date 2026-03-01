"""
SQL Server Utility Functions
Helper functions for database operations
"""

import pyodbc
import sys
from pathlib import Path

# Fix Unicode output on Windows terminals with cp1252 encoding
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Add setup directory to path
sys.path.append(str(Path(__file__).parent.parent / '01_Setup'))
from config import SQL_CONNECTION_STRING, SQL_TRUSTED_CONNECTION_STRING, SQL_SERVER_CONFIG

def ensure_database_exists():
    """
    Connect to master and create the WalmartUK database if it doesn't exist.
    Must be called before get_sql_connection() on a fresh SQL Server instance.
    """
    master_str = (
        f"DRIVER={SQL_SERVER_CONFIG['driver']};"
        f"SERVER={SQL_SERVER_CONFIG['server']};"
        f"DATABASE=master;"
        f"Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(master_str, autocommit=True)
        cursor = conn.cursor()
        cursor.execute(
            "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = ?) "
            "BEGIN CREATE DATABASE [WalmartUK] END",
            SQL_SERVER_CONFIG['database']
        )
        print(f"✓ Database '{SQL_SERVER_CONFIG['database']}' is ready")
        conn.close()
    except pyodbc.Error as e:
        print(f"❌ Could not ensure database exists: {e}")
        raise


def get_sql_connection(use_trusted=False):
    """
    Create and return SQL Server connection
    
    Args:
        use_trusted (bool): Use Windows Authentication if True
    
    Returns:
        pyodbc.Connection: Database connection object
    """
    try:
        if use_trusted:
            conn_str = SQL_TRUSTED_CONNECTION_STRING
        else:
            conn_str = SQL_CONNECTION_STRING
        
        conn = pyodbc.connect(conn_str)
        print("✓ Connected to SQL Server successfully")
        return conn
    
    except pyodbc.Error as e:
        print(f"❌ Error connecting to SQL Server: {e}")
        print("\nTroubleshooting:")
        print("1. Check server name in config.py")
        print("2. Verify username/password")
        print("3. Ensure SQL Server is running")
        print("4. Try use_trusted=True for Windows Authentication")
        raise

def execute_sql(conn, sql, params=None, commit=True):
    """
    Execute SQL statement with error handling
    
    Args:
        conn: Database connection
        sql (str): SQL statement to execute
        params (tuple): Parameters for parameterized query
        commit (bool): Commit transaction after execution
    
    Returns:
        cursor: Database cursor with results
    """
    cursor = conn.cursor()
    
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        
        if commit:
            conn.commit()
        
        return cursor
    
    except pyodbc.Error as e:
        print(f"❌ SQL Error: {e}")
        print(f"SQL: {sql[:100]}...")
        conn.rollback()
        raise

def execute_many(conn, sql, data_list, batch_size=1000, commit=True):
    """
    Execute parameterized SQL for multiple rows (batch insert)
    
    Args:
        conn: Database connection
        sql (str): INSERT statement with placeholders
        data_list (list): List of tuples containing row data
        batch_size (int): Number of rows per batch
        commit (bool): Commit after each batch
    
    Returns:
        int: Total rows inserted
    """
    cursor = conn.cursor()
    total_rows = 0
    
    try:
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            cursor.executemany(sql, batch)
            
            if commit:
                conn.commit()
            
            total_rows += len(batch)
            print(f"  Inserted {total_rows}/{len(data_list)} rows...", end='\r')
        
        print(f"\n✓ Inserted {total_rows} rows successfully")
        return total_rows
    
    except pyodbc.Error as e:
        print(f"\n❌ Batch insert error: {e}")
        conn.rollback()
        raise

def table_exists(conn, table_name):
    """
    Check if table exists in database
    
    Args:
        conn: Database connection
        table_name (str): Name of table to check
    
    Returns:
        bool: True if table exists
    """
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = '{table_name}'
    """)
    
    return cursor.fetchone()[0] > 0

def get_row_count(conn, table_name):
    """
    Get number of rows in table
    
    Args:
        conn: Database connection
        table_name (str): Name of table
    
    Returns:
        int: Number of rows
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
    return cursor.fetchone()[0]

def drop_table_if_exists(conn, table_name):
    """
    Drop table if it exists
    
    Args:
        conn: Database connection
        table_name (str): Name of table to drop
    """
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")
    conn.commit()
    print(f"✓ Dropped table {table_name} (if existed)")

def print_query_results(cursor, max_rows=10):
    """
    Pretty print query results
    
    Args:
        cursor: Database cursor with results
        max_rows (int): Maximum rows to display
    """
    # Get column names
    columns = [column[0] for column in cursor.description]
    
    # Fetch rows
    rows = cursor.fetchall()
    
    if not rows:
        print("(No results)")
        return
    
    # Calculate column widths
    col_widths = [len(col) for col in columns]
    for row in rows[:max_rows]:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))
    
    # Print header
    header = " | ".join(col.ljust(width) for col, width in zip(columns, col_widths))
    print(header)
    print("-" * len(header))
    
    # Print rows
    for row in rows[:max_rows]:
        print(" | ".join(str(val).ljust(width) for val, width in zip(row, col_widths)))
    
    if len(rows) > max_rows:
        print(f"... ({len(rows) - max_rows} more rows)")
    
    print(f"\nTotal rows: {len(rows)}")

def verify_database_setup(conn):
    """
    Verify all tables exist and have data
    
    Args:
        conn: Database connection
    
    Returns:
        dict: Table names and row counts
    """
    tables = ['Customer', 'Product', 'Store', 'Supplier', 'Order', 'OrderLine', 'Inventory']
    results = {}
    
    print("\nDatabase Verification:")
    print("=" * 50)
    
    for table in tables:
        if table_exists(conn, table):
            count = get_row_count(conn, table)
            results[table] = count
            status = "✓" if count > 0 else "⚠"
            print(f"{status} {table:20s} {count:>10,} rows")
        else:
            results[table] = 0
            print(f"❌ {table:20s} NOT FOUND")
    
    print("=" * 50)
    return results

# Test connection when run directly
if __name__ == "__main__":
    print("Testing SQL Server connection...")
    
    try:
        conn = get_sql_connection()
        print("\n✓ Connection successful!")
        
        # Test query
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"\nSQL Server Version:")
        print(version.split('\n')[0])  # First line only
        
        conn.close()
        print("\n✓ Connection closed")
        
    except Exception as e:
        print(f"\n❌ Connection test failed: {e}")
