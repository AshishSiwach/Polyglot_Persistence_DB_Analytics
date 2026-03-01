"""
STEP 1.3-1.6: SQL Server Demonstration Queries
Walmart UK Polyglot Persistence Project

Demonstrates:
1. Time-based analysis using OrderDate attribute (no Time entity)
2. Market basket analysis using Order/OrderLine structure (no Sales entity)
3. Inventory as junction table with explicit foreign keys
4. Customer lifetime value with complex joins
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from sql_utils import get_sql_connection, execute_sql, print_query_results

def demo_time_based_analysis(conn):
    """
    DEMO 1: Time-Based Analysis (OrderDate as Attribute)
    
    Shows: Time is an ATTRIBUTE, not a separate entity
    Feedback addressed: "Time is not an entity in its own right"
    """
    print("\n" + "=" * 70)
    print("DEMO 1: TIME-BASED ANALYSIS")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"Based on feedback, I removed the Time entity. OrderDate is now an')
    print('attribute in the Order table. Let me demonstrate time-based queries')
    print('using SQL\'s native date functions..."')
    print("\n" + "-" * 70)
    
    sql = """
    -- Monthly sales analysis using OrderDate attribute
    SELECT 
        YEAR(OrderDate) AS Year,
        MONTH(OrderDate) AS Month,
        DATENAME(MONTH, OrderDate) AS MonthName,
        COUNT(DISTINCT OrderID) AS NumOrders,
        COUNT(DISTINCT CustomerID) AS UniqueCustomers,
        SUM(TotalAmount) AS TotalRevenue,
        AVG(TotalAmount) AS AvgOrderValue,
        MIN(TotalAmount) AS MinOrder,
        MAX(TotalAmount) AS MaxOrder
    FROM [Order]
    WHERE OrderStatus = 'Completed'
    GROUP BY YEAR(OrderDate), MONTH(OrderDate), DATENAME(MONTH, OrderDate)
    ORDER BY Year, Month;
    """
    
    print("\nExecuting query...")
    print("-" * 70)
    cursor = execute_sql(conn, sql, commit=False)
    print_query_results(cursor, max_rows=20)
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("✓ No Time table needed - SQL's date functions work on OrderDate")
    print("✓ YEAR(), MONTH(), DATENAME() provide all temporal analysis")
    print("✓ Simpler structure, better performance")
    print("\nWhat to say in video:")
    print('"As you can see, using OrderDate as an attribute gives us powerful')
    print('time-based analytics without the overhead of a separate Time entity.') 
    print('This addresses the feedback that Time should not be a separate entity."')

def demo_market_basket_analysis(conn):
    """
    DEMO 2: Market Basket Analysis (Order/OrderLine Structure)
    
    Shows: Order + OrderLine replaces single Sales entity
    Feedback addressed: "Sales are not an entity in their own right"
    """
    print("\n\n" + "=" * 70)
    print("DEMO 2: MARKET BASKET ANALYSIS")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"Next, I\'ll demonstrate why replacing Sales with Order and OrderLine')
    print('is superior. This structure enables market basket analysis - finding')
    print('products frequently bought together..."')
    print("\n" + "-" * 70)
    
    sql = """
    -- Products frequently bought together
    SELECT TOP 10
        p1.ProductName AS Product1,
        p2.ProductName AS Product2,
        COUNT(*) AS TimesBoughtTogether,
        AVG(o.TotalAmount) AS AvgBasketValue
    FROM OrderLine ol1
    JOIN OrderLine ol2 
        ON ol1.OrderID = ol2.OrderID 
        AND ol1.ProductID < ol2.ProductID
    JOIN Product p1 ON ol1.ProductID = p1.ProductID
    JOIN Product p2 ON ol2.ProductID = p2.ProductID
    JOIN [Order] o ON ol1.OrderID = o.OrderID
    WHERE o.OrderStatus = 'Completed'
    GROUP BY p1.ProductName, p2.ProductName
    ORDER BY TimesBoughtTogether DESC;
    """
    
    print("\nExecuting query...")
    print("-" * 70)
    cursor = execute_sql(conn, sql, commit=False)
    print_query_results(cursor)
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("✓ This query identifies products in the SAME shopping basket")
    print("✓ Only possible with Order/OrderLine structure")
    print("✓ Single Sales entity couldn't represent multi-product orders")
    print("\nWhat to say in video:")
    print('"These results show which products customers buy together. This is')
    print('ONLY possible with the Order/OrderLine structure. With a single Sales')
    print('entity, we couldn\'t identify products in the same basket. This proves')
    print('why Sales should not be modeled as a single entity."')

def demo_inventory_junction_table(conn):
    """
    DEMO 3: Inventory as Junction Table
    
    Shows: Inventory with explicit foreign keys (not "via inventory")
    Feedback addressed: "Relationships should not be labelled 'via inventory'"
    """
    print("\n\n" + "=" * 70)
    print("DEMO 3: INVENTORY AS JUNCTION TABLE")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"The feedback noted relationships shouldn\'t be labeled \'via inventory.\'')
    print('Let me show how Inventory properly functions as a junction table with')
    print('explicit foreign key relationships..."')
    print("\n" + "-" * 70)
    
    sql = """
    -- Stock levels by store and supplier
    SELECT 
        s.StoreName,
        s.Region,
        p.ProductName,
        p.Category,
        sup.CompanyName AS Supplier,
        i.StockQuantity,
        i.ReorderLevel,
        i.CostPrice,
        p.Price AS RetailPrice,
        (p.Price - i.CostPrice) AS Margin,
        CASE 
            WHEN i.StockQuantity < i.ReorderLevel THEN 'REORDER NEEDED'
            WHEN i.StockQuantity < (i.ReorderLevel * 1.5) THEN 'Low Stock'
            ELSE 'Sufficient'
        END AS StockStatus
    FROM Inventory i
    JOIN Store s ON i.StoreID = s.StoreID
    JOIN Product p ON i.ProductID = p.ProductID
    JOIN Supplier sup ON i.SupplierID = sup.SupplierID
    WHERE s.StoreID = 1  -- Manchester store
    ORDER BY 
        CASE 
            WHEN i.StockQuantity < i.ReorderLevel THEN 1
            WHEN i.StockQuantity < (i.ReorderLevel * 1.5) THEN 2
            ELSE 3
        END,
        p.Category,
        p.ProductName;
    """
    
    print("\nExecuting query...")
    print("-" * 70)
    cursor = execute_sql(conn, sql, commit=False)
    print_query_results(cursor, max_rows=15)
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("✓ Inventory has THREE explicit foreign keys:")
    print("  • FK_Inventory_Store (StoreID)")
    print("  • FK_Inventory_Product (ProductID)")
    print("  • FK_Inventory_Supplier (SupplierID)")
    print("✓ These enable Many-to-Many relationships:")
    print("  • Store 1:M Inventory (one store, many products)")
    print("  • Product 1:M Inventory (one product, many stores)")
    print("  • Supplier 1:M Inventory (one supplier, many products)")
    print("\nWhat to say in video:")
    print('"Here you see Inventory linking THREE entities with explicit foreign')
    print('keys. This is NOT \'via inventory\' - these are proper relationships')
    print('with clear cardinality: Store 1:M Inventory, Product 1:M Inventory,')
    print('Supplier 1:M Inventory. The junction table enables M:M relationships."')

def demo_customer_lifetime_value(conn):
    """
    DEMO 4: Customer Lifetime Value
    
    Shows: Complex joins across multiple tables
    """
    print("\n\n" + "=" * 70)
    print("DEMO 4: CUSTOMER LIFETIME VALUE BY REGION")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"Now I\'ll demonstrate a complex join showing the power of relational')
    print('data - calculating customer lifetime value by region..."')
    print("\n" + "-" * 70)
    
    sql = """
    -- Customer lifetime value by city
    SELECT 
        c.City,
        COUNT(DISTINCT c.CustomerID) AS NumCustomers,
        COUNT(DISTINCT o.OrderID) AS TotalOrders,
        SUM(o.TotalAmount) AS TotalRevenue,
        AVG(o.TotalAmount) AS AvgOrderValue,
        SUM(o.TotalAmount) / COUNT(DISTINCT c.CustomerID) AS CustomerLifetimeValue,
        COUNT(DISTINCT o.OrderID) * 1.0 / COUNT(DISTINCT c.CustomerID) AS OrdersPerCustomer
    FROM Customer c
    LEFT JOIN [Order] o ON c.CustomerID = o.CustomerID
    WHERE o.OrderStatus = 'Completed' OR o.OrderID IS NULL
    GROUP BY c.City
    HAVING COUNT(DISTINCT o.OrderID) > 0
    ORDER BY CustomerLifetimeValue DESC;
    """
    
    print("\nExecuting query...")
    print("-" * 70)
    cursor = execute_sql(conn, sql, commit=False)
    print_query_results(cursor, max_rows=15)
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("✓ Clean joins across Customer and Order tables")
    print("✓ Calculates lifetime value per customer by region")
    print("✓ Shows benefit of proper relational structure")
    print("\nWhat to say in video:")
    print('"This query joins Customer and Order tables to calculate lifetime')
    print('value by city. Notice how clean the SQL is - this demonstrates the')
    print('power of proper relational structure with explicit foreign keys."')

def demo_common_identifier_verification(conn):
    """
    DEMO 5: Common Identifier Verification
    
    Shows: ProductIDs ready for MongoDB sync
    """
    print("\n\n" + "=" * 70)
    print("DEMO 5: COMMON IDENTIFIER VERIFICATION")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"Finally, let me verify our common identifiers - ProductID will be')
    print('the bridge between SQL Server and MongoDB..."')
    print("\n" + "-" * 70)
    
    sql = """
    -- ProductIDs for MongoDB synchronization
    SELECT 
        ProductID,
        ProductName,
        Category,
        Price,
        SKU
    FROM Product
    WHERE IsActive = 1
    ORDER BY ProductID;
    """
    
    print("\nExecuting query...")
    print("-" * 70)
    cursor = execute_sql(conn, sql, commit=False)
    print_query_results(cursor, max_rows=15)
    
    # Count ProductIDs
    sql_count = "SELECT COUNT(*) FROM Product WHERE IsActive = 1"
    cursor = execute_sql(conn, sql_count, commit=False)
    product_count = cursor.fetchone()[0]
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print(f"✓ SQL Server has {product_count} active products")
    print("✓ Each ProductID will match a MongoDB Product_catalogue document")
    print("✓ ProductID is our COMMON IDENTIFIER for polyglot persistence")
    print("\nWhat to say in video:")
    print(f'"SQL Server contains {product_count} active products. Each ProductID')
    print('shown here will exist in MongoDB as well - this is our common identifier')
    print('linking both databases. In the next section, I\'ll verify they match."')

def main():
    """Main execution function"""
    print("=" * 70)
    print("SQL SERVER DEMONSTRATION QUERIES")
    print("Walmart UK Polyglot Persistence Project")
    print("=" * 70)
    print("\nThis script demonstrates:")
    print("  1. Time as attribute (not entity)")
    print("  2. Order/OrderLine structure (not single Sales)")
    print("  3. Inventory junction table (explicit foreign keys)")
    print("  4. Complex joins for business insights")
    print("  5. Common identifiers for MongoDB sync")
    print("=" * 70)
    
    try:
        conn = get_sql_connection()
        
        # Run all demonstrations
        demo_time_based_analysis(conn)
        demo_market_basket_analysis(conn)
        demo_inventory_junction_table(conn)
        demo_customer_lifetime_value(conn)
        demo_common_identifier_verification(conn)
        
        print("\n\n" + "=" * 70)
        print("✓ ALL DEMONSTRATIONS COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print("\nKey Takeaways for Video:")
        print("  1. OrderDate attribute replaces Time entity")
        print("  2. Order/OrderLine enables market basket analysis")
        print("  3. Inventory has 3 explicit foreign keys (not 'via')")
        print("  4. Relational structure enables complex business queries")
        print("  5. ProductID ready for MongoDB synchronization")
        print("\nNext step: Run MongoDB scripts in 05_MongoDB/")
        
        conn.close()
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
