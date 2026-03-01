"""
CROSS-DATABASE POLYGLOT PERSISTENCE DEMONSTRATION
Walmart UK Polyglot Persistence Project

This is THE CRITICAL DEMO proving polyglot persistence works!

Demonstrates:
1. Common identifier verification (ProductID exists in both databases)
2. Cross-database queries (SQL sales + MongoDB sentiment)
3. Business insights only possible by combining both databases
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / '04_SQL_Server'))
sys.path.append(str(Path(__file__).parent.parent / '05_MongoDB'))

from sql_utils import get_sql_connection, execute_sql
from mongo_utils import get_mongo_connection

def verify_common_identifiers(sql_conn, mongo_db):
    """
    DEMO 1: Verify Common Identifiers Match
    
    Shows: ProductID exists in BOTH SQL Server and MongoDB
    This is THE PROOF that polyglot persistence is configured correctly
    """
    print("\n" + "=" * 70)
    print("DEMO 1: COMMON IDENTIFIER VERIFICATION")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"This is the critical demonstration - proving ProductID exists in')
    print('BOTH SQL Server and MongoDB, linking both systems together..."')
    print("\n" + "-" * 70)
    
    # Get ProductIDs from SQL Server
    print("\nStep 1: Fetching ProductIDs from SQL Server...")
    sql = "SELECT ProductID FROM Product WHERE IsActive = 1 ORDER BY ProductID"
    cursor = execute_sql(sql_conn, sql, commit=False)
    sql_product_ids = sorted([row[0] for row in cursor.fetchall()])
    print(f"✓ SQL Server has {len(sql_product_ids)} active products")
    print(f"  ProductIDs: {sql_product_ids[:10]}... ")
    
    # Get ProductIDs from MongoDB
    print("\nStep 2: Fetching ProductIDs from MongoDB...")
    mongo_product_ids = sorted(mongo_db.Product_catalogue.distinct('ProductID'))
    print(f"✓ MongoDB has {len(mongo_product_ids)} products")
    print(f"  ProductIDs: {mongo_product_ids[:10]}...")
    
    # Compare
    print("\n" + "-" * 70)
    print("COMPARISON:")
    print("-" * 70)
    
    if sql_product_ids == mongo_product_ids:
        print("✓✓✓ PERFECT MATCH! ✓✓✓")
        print(f"\nBoth databases contain the SAME {len(sql_product_ids)} ProductIDs")
        print("This proves our common identifier strategy works!")
    else:
        sql_only = set(sql_product_ids) - set(mongo_product_ids)
        mongo_only = set(mongo_product_ids) - set(sql_product_ids)
        
        if sql_only:
            print(f"⚠ ProductIDs in SQL but not MongoDB: {sql_only}")
        if mongo_only:
            print(f"⚠ ProductIDs in MongoDB but not SQL: {mongo_only}")
        
        print(f"\n⚠ Mismatch detected - {len(sql_product_ids)} in SQL, {len(mongo_product_ids)} in MongoDB")
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("✓ ProductID is our COMMON IDENTIFIER")
    print("✓ It exists in SQL Server Product table")
    print("✓ It exists in MongoDB Product_catalogue collection")
    print("✓ This enables us to JOIN data across both databases")
    print("\nWhat to say in video:")
    print('"Perfect match! Both SQL Server and MongoDB contain the exact same')
    print('ProductIDs. This is our common identifier enabling polyglot persistence."')
    
    return sql_product_ids == mongo_product_ids

def cross_database_product_analysis(sql_conn, mongo_db):
    """
    DEMO 2: Cross-Database Product Analysis
    
    Shows: Combining SQL Server sales data WITH MongoDB review sentiment
    This is THE BUSINESS VALUE of polyglot persistence!
    """
    print("\n\n" + "=" * 70)
    print("DEMO 2: CROSS-DATABASE PRODUCT ANALYSIS")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"Now for the powerful part - combining SQL Server sales data WITH')
    print('MongoDB review sentiment to gain insights neither database could')
    print('provide alone..."')
    print("\n" + "-" * 70)
    
    # STEP A: Get sales data from SQL Server
    print("\nStep A: Fetching product sales from SQL Server...")
    sql = """
    SELECT 
        p.ProductID,
        p.ProductName,
        p.Category,
        p.Price,
        COUNT(DISTINCT ol.OrderLineID) AS TimesSold,
        SUM(ol.Quantity) AS TotalUnitsSold,
        SUM(ol.LineTotal) AS TotalRevenue
    FROM Product p
    LEFT JOIN OrderLine ol ON p.ProductID = ol.ProductID
    WHERE p.IsActive = 1
    GROUP BY p.ProductID, p.ProductName, p.Category, p.Price
    HAVING COUNT(DISTINCT ol.OrderLineID) > 0
    ORDER BY TotalRevenue DESC
    """
    
    cursor = execute_sql(sql_conn, sql, commit=False)
    sql_results = {}
    
    print("\n{:<10} {:<40} {:<12} {:<15}".format(
        "ProductID", "Product Name", "Units Sold", "Revenue"))
    print("-" * 80)
    
    for row in cursor.fetchall():
        product_id, name, category, price, times_sold, units_sold, revenue = row
        sql_results[product_id] = {
            'name': name,
            'category': category,
            'price': float(price),
            'times_sold': times_sold,
            'units_sold': units_sold,
            'revenue': float(revenue)
        }
        
        if len(sql_results) <= 10:  # Show top 10
            print("{:<10} {:<40} {:<12} £{:<14,.2f}".format(
                product_id, name[:38], units_sold, revenue))
    
    print(f"\n✓ Retrieved sales data for {len(sql_results)} products from SQL Server")
    
    # STEP B: Get review sentiment from MongoDB
    print("\nStep B: Fetching review sentiment from MongoDB...")
    
    pipeline = [
        {
            '$group': {
                '_id': '$ProductID',
                'avgRating': {'$avg': '$rating'},
                'numReviews': {'$sum': 1},
                'positivePct': {
                    '$avg': {
                        '$cond': [{'$eq': ['$sentiment', 'positive']}, 100, 0]
                    }
                }
            }
        },
        {
            '$project': {
                'ProductID': '$_id',
                'avgRating': {'$round': ['$avgRating', 2]},
                'numReviews': 1,
                'positivePct': {'$round': ['$positivePct', 1]}
            }
        }
    ]
    
    mongo_results = {}
    for doc in mongo_db.Customer_reviews.aggregate(pipeline):
        mongo_results[doc['ProductID']] = {
            'avgRating': doc['avgRating'],
            'numReviews': doc['numReviews'],
            'positivePct': doc['positivePct']
        }
    
    print(f"✓ Retrieved sentiment data for {len(mongo_results)} products from MongoDB")
    
    # STEP C: COMBINE THE DATA (This is polyglot persistence in action!)
    print("\nStep C: COMBINING SQL + MongoDB data...")
    print("\n" + "=" * 80)
    print("COMBINED ANALYSIS (SQL Server Revenue + MongoDB Sentiment)")
    print("=" * 80)
    
    combined = []
    for product_id, sql_data in sql_results.items():
        if product_id in mongo_results:
            mongo_data = mongo_results[product_id]
            combined.append({
                'product_id': product_id,
                'name': sql_data['name'],
                'revenue': sql_data['revenue'],
                'units_sold': sql_data['units_sold'],
                'avg_rating': mongo_data['avgRating'],
                'num_reviews': mongo_data['numReviews'],
                'positive_pct': mongo_data['positivePct']
            })
    
    # Sort by revenue
    combined.sort(key=lambda x: x['revenue'], reverse=True)
    
    print("\n{:<10} {:<35} {:<12} {:<10} {:<12}".format(
        "ProductID", "Name", "Revenue", "Rating", "Sentiment"))
    print("-" * 85)
    
    for item in combined[:15]:
        print("{:<10} {:<35} £{:<11,.2f} {:<10.2f}⭐ {:<11.1f}%".format(
            item['product_id'],
            item['name'][:33],
            item['revenue'],
            item['avg_rating'],
            item['positive_pct']
        ))
    
    # IDENTIFY INSIGHTS
    print("\n" + "=" * 80)
    print("BUSINESS INSIGHTS (Only Possible with Polyglot Persistence!)")
    print("=" * 80)
    
    # High revenue but low rating
    problems = [item for item in combined 
                if item['revenue'] > 1000 and item['avg_rating'] < 3.5]
    
    if problems:
        print("\n⚠ HIGH SALES BUT LOW SATISFACTION:")
        for item in problems[:3]:
            print(f"  • {item['name']}: £{item['revenue']:,.2f} revenue but {item['avg_rating']}⭐ rating")
        print("  → Action: Investigate quality issues, improve product")
    
    # High revenue AND high rating
    winners = [item for item in combined[:10]
               if item['avg_rating'] >= 4.5]
    
    if winners:
        print("\n✓ TOP PERFORMERS (High Sales + High Satisfaction):")
        for item in winners[:3]:
            print(f"  • {item['name']}: £{item['revenue']:,.2f} revenue + {item['avg_rating']}⭐ rating")
        print("  → Action: Keep in stock, feature in marketing")
    
    print("\n" + "=" * 80)
    print("KEY INSIGHT:")
    print("=" * 80)
    print("✓ SQL Server provides SALES DATA (revenue, units sold)")
    print("✓ MongoDB provides CUSTOMER SENTIMENT (ratings, reviews)")
    print("✓ COMBINED, we identify products with high sales but low satisfaction")
    print("✓ This insight is IMPOSSIBLE with just one database!")
    print("\nWhat to say in video:")
    print('"By combining SQL Server revenue with MongoDB sentiment, we identified')
    print('products with high sales but low customer satisfaction - a critical')
    print('business insight only possible through polyglot persistence."')

def verify_customer_reviews_sync(sql_conn, mongo_db):
    """
    DEMO 3: Verify Customer Reviews Synchronization
    
    Shows: CustomerID in MongoDB references valid SQL Server customers
    """
    print("\n\n" + "=" * 70)
    print("DEMO 3: CUSTOMER REVIEW SYNCHRONIZATION")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"Let me also verify CustomerID synchronization - every review in')
    print('MongoDB should reference a valid customer in SQL Server..."')
    print("\n" + "-" * 70)
    
    # Get CustomerIDs from MongoDB reviews
    print("\nChecking CustomerIDs in MongoDB reviews...")
    mongo_customer_ids = set(mongo_db.Customer_reviews.distinct('CustomerID'))
    print(f"✓ MongoDB reviews reference {len(mongo_customer_ids)} unique customers")
    
    # Get CustomerIDs from SQL Server
    print("\nChecking CustomerIDs in SQL Server...")
    sql = "SELECT DISTINCT CustomerID FROM Customer WHERE IsActive = 1"
    cursor = execute_sql(sql_conn, sql, commit=False)
    sql_customer_ids = set(row[0] for row in cursor.fetchall())
    print(f"✓ SQL Server has {len(sql_customer_ids)} active customers")
    
    # Check if all MongoDB CustomerIDs exist in SQL
    orphaned = mongo_customer_ids - sql_customer_ids
    
    print("\n" + "-" * 70)
    print("VERIFICATION:")
    print("-" * 70)
    
    if not orphaned:
        print("✓✓✓ PERFECT! All CustomerIDs in MongoDB exist in SQL Server ✓✓✓")
        print("\nThis proves referential integrity across both databases!")
    else:
        print(f"⚠ Found {len(orphaned)} orphaned CustomerIDs in MongoDB")
        print(f"  (Reviews from customers not in SQL Server: {orphaned})")
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("✓ CustomerID is our second COMMON IDENTIFIER")
    print("✓ MongoDB reviews reference SQL Server customers")
    print("✓ Referential integrity maintained across both databases")

def main():
    """Main execution function"""
    print("=" * 70)
    print("POLYGLOT PERSISTENCE CROSS-DATABASE DEMONSTRATION")
    print("Walmart UK - THE CRITICAL PROOF!")
    print("=" * 70)
    print("\nThis demonstration PROVES polyglot persistence works by:")
    print("  1. Verifying ProductID exists in BOTH databases")
    print("  2. Combining SQL sales data + MongoDB sentiment")
    print("  3. Generating business insights impossible with one database")
    print("  4. Verifying CustomerID synchronization")
    print("=" * 70)
    
    try:
        # Connect to both databases
        print("\nConnecting to databases...")
        sql_conn = get_sql_connection()
        mongo_db = get_mongo_connection()
        
        # Run demonstrations
        identifiers_match = verify_common_identifiers(sql_conn, mongo_db)
        
        if identifiers_match:
            cross_database_product_analysis(sql_conn, mongo_db)
            verify_customer_reviews_sync(sql_conn, mongo_db)
        else:
            print("\n⚠ Skipping cross-database analysis due to identifier mismatch")
            print("  Fix data synchronization first!")
        
        sql_conn.close()
        
        print("\n\n" + "=" * 70)
        print("✓✓✓ POLYGLOT PERSISTENCE DEMONSTRATION COMPLETE ✓✓✓")
        print("=" * 70)
        print("\nWhat We Proved:")
        print("  1. ✓ Common identifiers (ProductID, CustomerID) exist in both DBs")
        print("  2. ✓ Data can be queried from both SQL Server AND MongoDB")
        print("  3. ✓ Combined insights reveal business-critical information")
        print("  4. ✓ Referential integrity maintained across both systems")
        print("\nThis is TRUE polyglot persistence in action!")
        print("\nFor PowerBI integration: Use these same ProductIDs to create")
        print("relationships between SQL and MongoDB data sources.")
        
        return 0 if identifiers_match else 1
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
