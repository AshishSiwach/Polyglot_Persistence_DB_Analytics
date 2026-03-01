"""
STEP 1.2: Import Sample Data to SQL Server
Walmart UK Polyglot Persistence Project

Imports CSV files generated from generate_walmart_data.py
"""

import sys
from pathlib import Path
import csv

sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent / '01_Setup'))

from sql_utils import get_sql_connection, execute_many, get_row_count
from config import CSV_FILES

def import_customers(conn):
    """Import customers from CSV"""
    print("\n1. Importing Customers...")
    
    csv_path = CSV_FILES['customers']
    if not csv_path.exists():
        print(f"   ❌ File not found: {csv_path}")
        return 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = []
        
        for row in reader:
            data.append((
                int(row['CustomerID']),
                row['CustomerName'],
                row['Email'],
                row['Phone'],
                row['Address'],
                row['City'],
                row['Postcode'],
                row['RegistrationDate'],
                int(row['IsActive'])
            ))
    
    sql = """
    SET IDENTITY_INSERT Customer ON;
    INSERT INTO Customer (CustomerID, CustomerName, Email, Phone, Address, City, Postcode, RegistrationDate, IsActive)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    SET IDENTITY_INSERT Customer OFF;
    """
    
    count = execute_many(conn, sql, data)
    print(f"   ✓ Imported {count} customers")
    return count

def import_products(conn):
    """Import products from CSV - COMMON IDENTIFIER"""
    print("\n2. Importing Products (Common Identifier)...")
    
    csv_path = CSV_FILES['products']
    if not csv_path.exists():
        print(f"   ❌ File not found: {csv_path}")
        return 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = []
        
        for row in reader:
            data.append((
                int(row['ProductID']),
                row['ProductName'],
                row['Category'],
                float(row['Price']),
                row['SKU'],
                row['Description'],
                int(row['IsActive']),
                row['CreatedDate']
            ))
    
    sql = """
    SET IDENTITY_INSERT Product ON;
    INSERT INTO Product (ProductID, ProductName, Category, Price, SKU, Description, IsActive, CreatedDate)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    SET IDENTITY_INSERT Product OFF;
    """
    
    count = execute_many(conn, sql, data)
    print(f"   ✓ Imported {count} products")
    print(f"   → ProductID will match MongoDB Product_catalogue")
    return count

def import_stores(conn):
    """Import stores from CSV"""
    print("\n3. Importing Stores...")
    
    csv_path = CSV_FILES['stores']
    if not csv_path.exists():
        print(f"   ❌ File not found: {csv_path}")
        return 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = []
        
        for row in reader:
            data.append((
                int(row['StoreID']),
                row['StoreName'],
                row['Location'],
                row['Region'],
                row['ManagerName'],
                row['Phone'],
                row['OpeningDate'] if row['OpeningDate'] else None,
                int(row['IsActive'])
            ))
    
    sql = """
    SET IDENTITY_INSERT Store ON;
    INSERT INTO Store (StoreID, StoreName, Location, Region, ManagerName, Phone, OpeningDate, IsActive)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    SET IDENTITY_INSERT Store OFF;
    """
    
    count = execute_many(conn, sql, data)
    print(f"   ✓ Imported {count} stores")
    return count

def import_suppliers(conn):
    """Import suppliers from CSV"""
    print("\n4. Importing Suppliers...")
    
    csv_path = CSV_FILES['suppliers']
    if not csv_path.exists():
        print(f"   ❌ File not found: {csv_path}")
        return 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = []
        
        for row in reader:
            data.append((
                int(row['SupplierID']),
                row['CompanyName'],
                row['ContactName'],
                row['ContactEmail'],
                row['Phone'],
                row['City'],
                row['Country'],
                int(row['IsActive'])
            ))
    
    sql = """
    SET IDENTITY_INSERT Supplier ON;
    INSERT INTO Supplier (SupplierID, CompanyName, ContactName, ContactEmail, Phone, City, Country, IsActive)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    SET IDENTITY_INSERT Supplier OFF;
    """
    
    count = execute_many(conn, sql, data)
    print(f"   ✓ Imported {count} suppliers")
    return count

def import_orders(conn):
    """Import orders from CSV"""
    print("\n5. Importing Orders...")
    
    csv_path = CSV_FILES['orders']
    if not csv_path.exists():
        print(f"   ❌ File not found: {csv_path}")
        return 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = []
        
        for row in reader:
            data.append((
                int(row['OrderID']),
                int(row['CustomerID']),
                int(row['StoreID']),
                row['OrderDate'],
                float(row['TotalAmount']),
                row['OrderStatus'],
                row['PaymentMethod']
            ))
    
    sql = """
    SET IDENTITY_INSERT [Order] ON;
    INSERT INTO [Order] (OrderID, CustomerID, StoreID, OrderDate, TotalAmount, OrderStatus, PaymentMethod)
    VALUES (?, ?, ?, ?, ?, ?, ?);
    SET IDENTITY_INSERT [Order] OFF;
    """
    
    count = execute_many(conn, sql, data)
    print(f"   ✓ Imported {count} orders")
    return count

def import_orderlines(conn):
    """Import order lines from CSV"""
    print("\n6. Importing Order Lines...")
    
    csv_path = CSV_FILES['orderlines']
    if not csv_path.exists():
        print(f"   ❌ File not found: {csv_path}")
        return 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = []
        
        for row in reader:
            data.append((
                int(row['OrderLineID']),
                int(row['OrderID']),
                int(row['ProductID']),
                int(row['Quantity']),
                float(row['UnitPrice'])
                # LineTotal is calculated automatically
            ))
    
    sql = """
    SET IDENTITY_INSERT OrderLine ON;
    INSERT INTO OrderLine (OrderLineID, OrderID, ProductID, Quantity, UnitPrice)
    VALUES (?, ?, ?, ?, ?);
    SET IDENTITY_INSERT OrderLine OFF;
    """
    
    count = execute_many(conn, sql, data)
    print(f"   ✓ Imported {count} order lines")
    return count

def import_inventory(conn):
    """Import inventory from CSV"""
    print("\n7. Importing Inventory...")
    
    csv_path = CSV_FILES['inventory']
    if not csv_path.exists():
        print(f"   ❌ File not found: {csv_path}")
        return 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = []
        
        for row in reader:
            data.append((
                int(row['InventoryID']),
                int(row['ProductID']),
                int(row['StoreID']),
                int(row['SupplierID']),
                int(row['StockQuantity']),
                int(row['ReorderLevel']),
                int(row['MaxStockLevel']),
                row['LastRestocked'] if row['LastRestocked'] else None,
                float(row['CostPrice']) if row['CostPrice'] else None
            ))
    
    sql = """
    SET IDENTITY_INSERT Inventory ON;
    INSERT INTO Inventory (InventoryID, ProductID, StoreID, SupplierID, StockQuantity, ReorderLevel, MaxStockLevel, LastRestocked, CostPrice)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    SET IDENTITY_INSERT Inventory OFF;
    """
    
    count = execute_many(conn, sql, data)
    print(f"   ✓ Imported {count} inventory records")
    return count

def verify_import(conn):
    """Verify data was imported correctly"""
    print("\n" + "=" * 70)
    print("VERIFICATION: Row Counts")
    print("=" * 70)
    
    tables = {
        'Customer': 'customers',
        'Product': 'products', 
        'Store': 'stores',
        'Supplier': 'suppliers',
        'Order': 'orders',
        'OrderLine': 'orderlines',
        'Inventory': 'inventory'
    }
    
    all_correct = True
    
    for table_name, csv_key in tables.items():
        db_count = get_row_count(conn, table_name)
        
        # Count CSV rows
        csv_path = CSV_FILES[csv_key]
        if csv_path.exists():
            with open(csv_path, 'r', encoding='utf-8') as f:
                csv_count = sum(1 for _ in f) - 1  # Subtract header
        else:
            csv_count = 0
        
        match = "✓" if db_count == csv_count else "❌"
        print(f"{match} {table_name:15s} - DB: {db_count:>6,} | CSV: {csv_count:>6,}")
        
        if db_count != csv_count:
            all_correct = False
    
    print("=" * 70)
    return all_correct

def main():
    """Main execution function"""
    print("=" * 70)
    print("STEP 1.2: IMPORT SAMPLE DATA TO SQL SERVER")
    print("=" * 70)
    print(f"\nData directory: {CSV_FILES['customers'].parent}")
    print("\nNote: Ensure you've run generate_walmart_data.py first!")
    print("=" * 70)
    
    try:
        # Connect to SQL Server
        conn = get_sql_connection()
        
        # Import all data
        import_customers(conn)
        import_products(conn)
        import_stores(conn)
        import_suppliers(conn)
        import_orders(conn)
        import_orderlines(conn)
        import_inventory(conn)
        
        # Verify
        if verify_import(conn):
            print("\n✓ SUCCESS! All data imported correctly")
            print("\nNext step: Run 03_demo_queries.py to see the data in action")
        else:
            print("\n⚠ WARNING: Row count mismatch detected")
            print("Check CSV files and database tables")
        
        conn.close()
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
