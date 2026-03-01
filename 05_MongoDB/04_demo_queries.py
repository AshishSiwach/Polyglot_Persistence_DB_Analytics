"""
STEP 2.4-2.5: MongoDB Demonstration Queries
Walmart UK Polyglot Persistence Project

Demonstrates:
1. Schema flexibility (different products, different attributes)
2. Sentiment analysis aggregation
3. Text search on reviews
4. Common identifiers for SQL Server sync
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from mongo_utils import get_mongo_connection, print_documents

def demo_schema_flexibility(db):
    """
    DEMO 1: Schema Flexibility
    
    Shows: Different products have different attributes
    """
    print("\n" + "=" * 70)
    print("DEMO 1: SCHEMA FLEXIBILITY")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"Let me demonstrate MongoDB\'s schema flexibility. Notice how')
    print('different products have different attributes - this would be')
    print('difficult in SQL..."')
    print("\n" + "-" * 70)
    
    # Get examples: Food product vs Electronics
    print("\nFetching products with different structures...")
    print("-" * 70)
    
    # Find a food product (has nutritional_info)
    print("\n📦 FOOD PRODUCT (has nutritional_info):")
    cursor = db.Product_catalogue.find(
        {'nutritional_info': {'$exists': True}},
        limit=1
    )
    print_documents(cursor, max_docs=1)
    
    # Find an electronics product (has different specs)
    print("\n📦 ELECTRONICS PRODUCT (different specifications):")
    cursor = db.Product_catalogue.find(
        {'ProductID': 5},  # Samsung TV from our data
        limit=1
    )
    print_documents(cursor, max_docs=1)
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("✓ Food products have 'nutritional_info' (calories, protein, fat)")
    print("✓ Electronics have different specs (screen_size, resolution)")
    print("✓ MongoDB handles this naturally - no rigid schema needed")
    print("✓ In SQL, we'd need separate tables for each product category")
    print("\nWhat to say in video:")
    print('"See how Product 1 (food) has nutritional_info, but Product 5')
    print('(electronics) has screen_size and smart_features instead. MongoDB')
    print('handles this flexibility naturally with its document model."')

def demo_sentiment_analysis(db):
    """
    DEMO 2: Sentiment Analysis Aggregation
    
    Shows: Aggregation pipeline calculating average ratings and sentiment
    """
    print("\n\n" + "=" * 70)
    print("DEMO 2: SENTIMENT ANALYSIS AGGREGATION")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"Now I\'ll use MongoDB\'s aggregation pipeline to analyze customer')
    print('sentiment by product..."')
    print("\n" + "-" * 70)
    
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
                },
                'negativePct': {
                    '$avg': {
                        '$cond': [{'$eq': ['$sentiment', 'negative']}, 100, 0]
                    }
                }
            }
        },
        {
            '$match': {
                'numReviews': {'$gte': 5}  # Only products with 5+ reviews
            }
        },
        {
            '$sort': {'avgRating': -1}
        },
        {
            '$limit': 10
        },
        {
            '$project': {
                '_id': 0,
                'ProductID': '$_id',
                'avgRating': {'$round': ['$avgRating', 2]},
                'numReviews': 1,
                'positivePct': {'$round': ['$positivePct', 1]},
                'negativePct': {'$round': ['$negativePct', 1]}
            }
        }
    ]
    
    print("\nExecuting aggregation pipeline...")
    print("-" * 70)
    cursor = db.Customer_reviews.aggregate(pipeline)
    
    results = list(cursor)
    print("\nTop-Rated Products (min 5 reviews):")
    print("-" * 70)
    print(f"{'ProductID':<12} {'Avg Rating':<12} {'Reviews':<10} {'Positive %':<12} {'Negative %':<12}")
    print("-" * 70)
    
    for doc in results:
        print(f"{doc['ProductID']:<12} {doc['avgRating']:<12.2f} {doc['numReviews']:<10} "
              f"{doc['positivePct']:<12.1f} {doc['negativePct']:<12.1f}")
    
    print(f"\nTotal products analyzed: {len(results)}")
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("✓ Aggregation pipeline groups reviews by ProductID")
    print("✓ Calculates average rating and sentiment percentages")
    print("✓ ProductID matches SQL Server Product table")
    print("\nWhat to say in video:")
    print('"This aggregation groups reviews by ProductID and calculates')
    print('average rating plus sentiment percentages. Notice ProductID appears')
    print('here - this is the same ProductID in SQL Server, our common identifier."')

def demo_text_search(db):
    """
    DEMO 3: Full-Text Search on Reviews
    
    Shows: Text index in action
    """
    print("\n\n" + "=" * 70)
    print("DEMO 3: FULL-TEXT SEARCH ON REVIEWS")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"MongoDB\'s text index allows full-text search on review content..."')
    print("\n" + "-" * 70)
    
    search_term = "excellent quality"
    
    print(f'\nSearching for reviews containing: "{search_term}"')
    print("-" * 70)
    
    cursor = db.Customer_reviews.find(
        {'$text': {'$search': search_term}},
        {'ProductID': 1, 'CustomerID': 1, 'rating': 1, 'review_text': 1, 
         'sentiment': 1, '_id': 0}
    ).limit(5)
    
    results = list(cursor)
    
    for i, doc in enumerate(results, 1):
        print(f"\n--- Review {i} ---")
        print(f"ProductID: {doc['ProductID']} | CustomerID: {doc['CustomerID']} | Rating: {doc['rating']}⭐")
        print(f"Sentiment: {doc['sentiment'].upper()}")
        print(f"Text: {doc['review_text'][:100]}...")
    
    print(f"\nFound {len(results)} reviews (showing first 5)")
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("✓ Text index enables full-text search on review content")
    print("✓ Much faster than scanning all documents")
    print("✓ Results include ProductID and CustomerID (common identifiers)")

def demo_common_identifier_verification(db):
    """
    DEMO 4: Common Identifier Verification
    
    Shows: ProductIDs in MongoDB ready for SQL sync
    """
    print("\n\n" + "=" * 70)
    print("DEMO 4: COMMON IDENTIFIER VERIFICATION")
    print("=" * 70)
    print("\nWhat to say in video:")
    print('"Let me verify our common identifiers - ProductID exists in MongoDB')
    print('and matches SQL Server..."')
    print("\n" + "-" * 70)
    
    # Get distinct ProductIDs from Product_catalogue
    product_ids_catalogue = sorted(db.Product_catalogue.distinct('ProductID'))
    
    # Get distinct ProductIDs from Customer_reviews
    product_ids_reviews = sorted(db.Customer_reviews.distinct('ProductID'))
    
    print("\nProductIDs in Product_catalogue:")
    print(f"{product_ids_catalogue[:10]}... ({len(product_ids_catalogue)} total)")
    
    print("\nProductIDs referenced in Customer_reviews:")
    print(f"{product_ids_reviews[:10]}... ({len(product_ids_reviews)} total)")
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print(f"✓ Product_catalogue has {len(product_ids_catalogue)} ProductIDs")
    print(f"✓ Customer_reviews references {len(product_ids_reviews)} ProductIDs")
    print("✓ These ProductIDs MUST match SQL Server Product table")
    print("✓ This is our COMMON IDENTIFIER for polyglot persistence")
    print("\nWhat to say in video:")
    print(f'"MongoDB contains {len(product_ids_catalogue)} products. Each ProductID')
    print('shown here exists in SQL Server as well. In the next demo, I\'ll')
    print('verify they match by querying both databases."')

def main():
    """Main execution function"""
    print("=" * 70)
    print("MONGODB DEMONSTRATION QUERIES")
    print("Walmart UK Polyglot Persistence Project")
    print("=" * 70)
    print("\nThis script demonstrates:")
    print("  1. Schema flexibility (different attributes per product)")
    print("  2. Sentiment analysis with aggregation pipeline")
    print("  3. Full-text search on reviews")
    print("  4. Common identifiers for SQL Server sync")
    print("=" * 70)
    
    try:
        db = get_mongo_connection()
        
        # Run all demonstrations
        demo_schema_flexibility(db)
        demo_sentiment_analysis(db)
        demo_text_search(db)
        demo_common_identifier_verification(db)
        
        print("\n\n" + "=" * 70)
        print("✓ ALL DEMONSTRATIONS COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print("\nKey Takeaways for Video:")
        print("  1. MongoDB handles flexible schemas naturally")
        print("  2. Aggregation pipeline enables complex analytics")
        print("  3. Text indexes support full-text search")
        print("  4. ProductID and CustomerID link to SQL Server")
        print("\nNext step: Run cross_database_demo.py in 06_Polyglot_Demo/")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
