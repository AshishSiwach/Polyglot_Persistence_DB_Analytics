"""
STEP 1.1: Create SQL Server Database Tables
Walmart UK Polyglot Persistence Project

Demonstrates:
- Time as ATTRIBUTE (OrderDate), not a separate entity
- Sales replaced with Order + OrderLine structure
- Inventory as proper junction table with explicit foreign keys
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from sql_utils import get_sql_connection, execute_sql, table_exists

def create_customer_table(conn):
    """Create Customer table"""
    print("\n1. Creating Customer table...")
    
    sql = """
    CREATE TABLE Customer (
        CustomerID INT PRIMARY KEY IDENTITY(1,1),
        CustomerName NVARCHAR(100) NOT NULL,
        Email NVARCHAR(100) UNIQUE NOT NULL,
        Phone NVARCHAR(20),
        Address NVARCHAR(200),
        City NVARCHAR(50),
        Postcode NVARCHAR(10),
        RegistrationDate DATE DEFAULT GETDATE(),
        IsActive BIT DEFAULT 1,
        
        CONSTRAINT CK_Customer_Email CHECK (Email LIKE '%@%.%'),
        CONSTRAINT CK_Customer_Postcode CHECK (LEN(Postcode) >= 5)
    )
    """
    
    execute_sql(conn, sql)
    execute_sql(conn, "CREATE INDEX idx_Customer_Email ON Customer(Email)")
    execute_sql(conn, "CREATE INDEX idx_Customer_City ON Customer(City)")
    print("   ✓ Customer table created")

def create_product_table(conn):
    """Create Product table - COMMON IDENTIFIER with MongoDB"""
    print("\n2. Creating Product table (Common Identifier)...")
    
    sql = """
    CREATE TABLE Product (
        ProductID INT PRIMARY KEY IDENTITY(1,1),
        ProductName NVARCHAR(100) NOT NULL,
        Category NVARCHAR(50) NOT NULL,
        Price DECIMAL(10,2) NOT NULL,
        SKU NVARCHAR(50) UNIQUE,
        Description NVARCHAR(500),
        IsActive BIT DEFAULT 1,
        CreatedDate DATE DEFAULT GETDATE(),
        
        CONSTRAINT CK_Product_Price CHECK (Price > 0),
        CONSTRAINT CK_Product_Category CHECK (
            Category IN ('Fresh Food', 'Bakery', 'Grocery', 'Confectionery', 
                         'Electronics', 'Home', 'Toys', 'Clothing')
        )
    )
    """
    
    execute_sql(conn, sql)
    execute_sql(conn, "CREATE INDEX idx_Product_Category ON Product(Category)")
    print("   ✓ Product table created")
    print("   → ProductID is COMMON IDENTIFIER with MongoDB Product_catalogue")

def create_store_table(conn):
    """Create Store table"""
    print("\n3. Creating Store table...")
    
    sql = """
    CREATE TABLE Store (
        StoreID INT PRIMARY KEY IDENTITY(1,1),
        StoreName NVARCHAR(100) NOT NULL,
        Location NVARCHAR(100) NOT NULL,
        Region NVARCHAR(50) NOT NULL,
        ManagerName NVARCHAR(100),
        Phone NVARCHAR(20),
        OpeningDate DATE,
        IsActive BIT DEFAULT 1,
        
        CONSTRAINT CK_Store_Region CHECK (
            Region IN ('North West', 'North East', 'Yorkshire', 'West Midlands', 
                       'East Midlands', 'East of England', 'Greater London', 'South East', 
                       'South West', 'Wales', 'Scotland', 'Northern Ireland')
        )
    )
    """
    
    execute_sql(conn, sql)
    execute_sql(conn, "CREATE INDEX idx_Store_Region ON Store(Region)")
    print("   ✓ Store table created")

def create_supplier_table(conn):
    """Create Supplier table"""
    print("\n4. Creating Supplier table...")
    
    sql = """
    CREATE TABLE Supplier (
        SupplierID INT PRIMARY KEY IDENTITY(1,1),
        CompanyName NVARCHAR(100) NOT NULL,
        ContactName NVARCHAR(100),
        ContactEmail NVARCHAR(100) UNIQUE NOT NULL,
        Phone NVARCHAR(20),
        City NVARCHAR(50),
        Country NVARCHAR(50) DEFAULT 'United Kingdom',
        IsActive BIT DEFAULT 1
    )
    """
    
    execute_sql(conn, sql)
    print("   ✓ Supplier table created")

def create_order_table(conn):
    """Create Order table - TIME AS ATTRIBUTE (not separate entity)"""
    print("\n5. Creating Order table (Time as Attribute)...")
    
    sql = """
    CREATE TABLE [Order] (
        OrderID INT PRIMARY KEY IDENTITY(1,1),
        CustomerID INT NOT NULL,
        StoreID INT NOT NULL,
        OrderDate DATETIME DEFAULT GETDATE(),  -- TIME AS ATTRIBUTE!
        TotalAmount DECIMAL(10,2) NOT NULL DEFAULT 0,
        OrderStatus NVARCHAR(20) DEFAULT 'Pending',
        PaymentMethod NVARCHAR(50),
        
        -- Foreign Keys (explicit relationships)
        CONSTRAINT FK_Order_Customer FOREIGN KEY (CustomerID) 
            REFERENCES Customer(CustomerID),
        CONSTRAINT FK_Order_Store FOREIGN KEY (StoreID) 
            REFERENCES Store(StoreID),
        
        -- Constraints
        CONSTRAINT CK_Order_TotalAmount CHECK (TotalAmount >= 0),
        CONSTRAINT CK_Order_Status CHECK (
            OrderStatus IN ('Pending', 'Processing', 'Completed', 'Cancelled', 'Refunded')
        )
    )
    """
    
    execute_sql(conn, sql)
    execute_sql(conn, "CREATE INDEX idx_Order_Customer ON [Order](CustomerID)")
    execute_sql(conn, "CREATE INDEX idx_Order_Store ON [Order](StoreID)")
    execute_sql(conn, "CREATE INDEX idx_Order_Date ON [Order](OrderDate)")
    print("   ✓ Order table created")
    print("   → OrderDate is an ATTRIBUTE (no separate Time entity)")
    print("   → Replaces old 'Sales' entity with proper transaction header")

def create_orderline_table(conn):
    """Create OrderLine table - Transaction detail"""
    print("\n6. Creating OrderLine table (Transaction Detail)...")
    
    sql = """
    CREATE TABLE OrderLine (
        OrderLineID INT PRIMARY KEY IDENTITY(1,1),
        OrderID INT NOT NULL,
        ProductID INT NOT NULL,
        Quantity INT NOT NULL,
        UnitPrice DECIMAL(10,2) NOT NULL,
        LineTotal AS (Quantity * UnitPrice) PERSISTED,  -- Calculated column
        
        -- Foreign Keys (explicit relationships)
        CONSTRAINT FK_OrderLine_Order FOREIGN KEY (OrderID) 
            REFERENCES [Order](OrderID) ON DELETE CASCADE,
        CONSTRAINT FK_OrderLine_Product FOREIGN KEY (ProductID) 
            REFERENCES Product(ProductID),
        
        -- Constraints
        CONSTRAINT CK_OrderLine_Quantity CHECK (Quantity > 0),
        CONSTRAINT CK_OrderLine_UnitPrice CHECK (UnitPrice >= 0)
    )
    """
    
    execute_sql(conn, sql)
    execute_sql(conn, "CREATE INDEX idx_OrderLine_Order ON OrderLine(OrderID)")
    execute_sql(conn, "CREATE INDEX idx_OrderLine_Product ON OrderLine(ProductID)")
    print("   ✓ OrderLine table created")
    print("   → Order + OrderLine = proper header-detail transaction model")

def create_inventory_table(conn):
    """Create Inventory table - JUNCTION TABLE for M:M relationships"""
    print("\n7. Creating Inventory table (Junction Table)...")
    
    sql = """
    CREATE TABLE Inventory (
        InventoryID INT PRIMARY KEY IDENTITY(1,1),
        ProductID INT NOT NULL,
        StoreID INT NOT NULL,
        SupplierID INT NOT NULL,
        StockQuantity INT NOT NULL DEFAULT 0,
        ReorderLevel INT DEFAULT 10,
        MaxStockLevel INT DEFAULT 1000,
        LastRestocked DATE,
        CostPrice DECIMAL(10,2),
        
        -- Foreign Keys (THREE explicit relationships!)
        CONSTRAINT FK_Inventory_Product FOREIGN KEY (ProductID) 
            REFERENCES Product(ProductID),
        CONSTRAINT FK_Inventory_Store FOREIGN KEY (StoreID) 
            REFERENCES Store(StoreID),
        CONSTRAINT FK_Inventory_Supplier FOREIGN KEY (SupplierID) 
            REFERENCES Supplier(SupplierID),
        
        -- Constraints
        CONSTRAINT CK_Inventory_StockQuantity CHECK (StockQuantity >= 0),
        CONSTRAINT CK_Inventory_ReorderLevel CHECK (ReorderLevel >= 0),
        CONSTRAINT CK_Inventory_MaxStockLevel CHECK (MaxStockLevel >= ReorderLevel),
        
        -- Unique constraint: No duplicate Product-Store-Supplier combinations
        CONSTRAINT UQ_Inventory_Product_Store_Supplier 
            UNIQUE (ProductID, StoreID, SupplierID)
    )
    """
    
    execute_sql(conn, sql)
    execute_sql(conn, "CREATE INDEX idx_Inventory_Product ON Inventory(ProductID)")
    execute_sql(conn, "CREATE INDEX idx_Inventory_Store ON Inventory(StoreID)")
    execute_sql(conn, "CREATE INDEX idx_Inventory_Supplier ON Inventory(SupplierID)")
    print("   ✓ Inventory table created")
    print("   → Inventory is JUNCTION TABLE enabling:")
    print("      • Store → Inventory (1:M)")
    print("      • Product → Inventory (1:M)")
    print("      • Supplier → Inventory (1:M)")
    print("   → NO 'via inventory' labels - these are EXPLICIT foreign keys!")

def main():
    """Main execution function"""
    print("=" * 70)
    print("STEP 1.1: CREATE SQL SERVER DATABASE TABLES")
    print("=" * 70)
    print("\nAddressing Feedback:")
    print("  ✓ Time as ATTRIBUTE (OrderDate), not separate entity")
    print("  ✓ Sales replaced with Order + OrderLine")
    print("  ✓ Inventory with EXPLICIT foreign key relationships")
    print("=" * 70)
    
    try:
        # Connect to SQL Server
        conn = get_sql_connection()
        
        # Create tables in dependency order
        create_customer_table(conn)
        create_product_table(conn)
        create_store_table(conn)
        create_supplier_table(conn)
        create_order_table(conn)
        create_orderline_table(conn)
        create_inventory_table(conn)
        
        # Verify creation
        print("\n" + "=" * 70)
        print("VERIFICATION: Checking all tables exist...")
        print("=" * 70)
        
        tables = ['Customer', 'Product', 'Store', 'Supplier', 'Order', 'OrderLine', 'Inventory']
        all_exist = True
        
        for table in tables:
            exists = table_exists(conn, table)
            status = "✓" if exists else "❌"
            print(f"{status} {table}")
            if not exists:
                all_exist = False
        
        conn.close()
        
        if all_exist:
            print("\n" + "=" * 70)
            print("✓ SUCCESS! All 7 tables created successfully")
            print("=" * 70)
            print("\nKey Improvements:")
            print("  1. NO Time entity - OrderDate is an attribute in Order table")
            print("  2. NO Sales entity - replaced with Order + OrderLine structure")
            print("  3. Inventory has 3 explicit foreign keys (not 'via inventory')")
            print("\nNext step: Run 02_import_data.py to load sample data")
        else:
            print("\n❌ Some tables failed to create. Check errors above.")
            return 1
        
        return 0
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
