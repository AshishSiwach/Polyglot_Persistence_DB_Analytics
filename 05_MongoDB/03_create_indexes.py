"""
STEP 2.3: Create MongoDB Indexes
Walmart UK Polyglot Persistence Project

Creates indexes on common identifiers (ProductID, CustomerID)
and other frequently queried fields for performance
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from mongo_utils import get_mongo_connection, create_index

def create_product_catalogue_indexes(db):
    """Create indexes on Product_catalogue collection"""
    print("\n1. Creating indexes on Product_catalogue...")
    
    collection_name = 'Product_catalogue'
    
    # Index on ProductID (COMMON IDENTIFIER - most important!)
    create_index(
        db, collection_name, 
        'ProductID', 
        unique=True, 
        name='idx_productid_unique'
    )
    print("   → ProductID index enables fast joins with SQL Server")
    
    # Index on availability (for filtering stock status)
    create_index(
        db, collection_name,
        'availability',
        name='idx_availability'
    )
    print("   → Availability index for filtering in-stock products")
    
    print(f"   ✓ All indexes created on {collection_name}")

def create_customer_reviews_indexes(db):
    """Create indexes on Customer_reviews collection"""
    print("\n2. Creating indexes on Customer_reviews...")
    
    collection_name = 'Customer_reviews'
    
    # Compound index on ProductID + review_date (for sorting reviews by product)
    create_index(
        db, collection_name,
        [('ProductID', 1), ('review_date', -1)],
        name='idx_product_date'
    )
    print("   → ProductID+date index for fetching recent reviews per product")
    
    # Index on CustomerID (COMMON IDENTIFIER)
    create_index(
        db, collection_name,
        'CustomerID',
        name='idx_customerid'
    )
    print("   → CustomerID index enables joins with SQL Server Customer table")
    
    # Text index on review_text (for search functionality)
    create_index(
        db, collection_name,
        [('review_text', 'text')],
        name='idx_review_text_search'
    )
    print("   → Text index enables full-text search on reviews")
    
    # Index on sentiment (for filtering positive/negative reviews)
    create_index(
        db, collection_name,
        'sentiment',
        name='idx_sentiment'
    )
    print("   → Sentiment index for filtering by positive/negative")
    
    # Index on rating (for filtering by star rating)
    create_index(
        db, collection_name,
        'rating',
        name='idx_rating'
    )
    print("   → Rating index for filtering by stars (1-5)")
    
    print(f"   ✓ All indexes created on {collection_name}")

def verify_indexes(db):
    """Verify all indexes were created"""
    print("\n" + "=" * 70)
    print("VERIFICATION: Listing all indexes")
    print("=" * 70)
    
    collections = ['Product_catalogue', 'Customer_reviews']
    
    for collection_name in collections:
        print(f"\n{collection_name}:")
        indexes = db[collection_name].list_indexes()
        
        for idx in indexes:
            index_name = idx.get('name', 'unknown')
            keys = idx.get('key', {})
            unique = idx.get('unique', False)
            
            # Format keys nicely
            key_str = ', '.join([f"{k}: {v}" for k, v in keys.items()])
            unique_str = " (UNIQUE)" if unique else ""
            
            print(f"  • {index_name}: {key_str}{unique_str}")
    
    print("\n" + "=" * 70)

def main():
    """Main execution function"""
    print("=" * 70)
    print("STEP 2.3: CREATE MONGODB INDEXES")
    print("=" * 70)
    print("\nIndexing Strategy:")
    print("  • ProductID: UNIQUE index (common identifier with SQL)")
    print("  • CustomerID: Index for joins with SQL Server")
    print("  • review_date: For sorting by recency")
    print("  • sentiment/rating: For filtering reviews")
    print("  • review_text: Full-text search capability")
    print("=" * 70)
    
    try:
        # Connect to MongoDB
        db = get_mongo_connection()
        
        # Create indexes
        create_product_catalogue_indexes(db)
        create_customer_reviews_indexes(db)
        
        # Verify
        verify_indexes(db)
        
        print("\n✓ SUCCESS! All indexes created")
        print("\nPerformance Benefits:")
        print("  1. ProductID index → Fast lookups when joining with SQL")
        print("  2. CustomerID index → Fast customer review retrieval")
        print("  3. Compound indexes → Efficient sorting and filtering")
        print("  4. Text index → Full-text search on reviews")
        print("\nNext step: Run 04_demo_queries.py to test MongoDB queries")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
