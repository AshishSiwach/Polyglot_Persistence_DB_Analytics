"""
STEP 2.1: Create MongoDB Collections
Walmart UK Polyglot Persistence Project

Creates collections with schema validation for:
- Product_catalogue (flexible product data)
- Customer_reviews (user-generated content)

Both linked to SQL Server via common identifiers (ProductID, CustomerID)
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from mongo_utils import get_mongo_connection, collection_exists, drop_collection_if_exists

def create_product_catalogue_collection(db):
    """
    Create Product_catalogue collection
    
    Stores: Product images, specifications, nutritional info
    Common Identifier: ProductID (links to SQL Server Product table)
    """
    print("\n1. Creating Product_catalogue collection...")
    
    collection_name = 'Product_catalogue'
    
    # Drop if exists (for clean setup)
    if collection_exists(db, collection_name):
        print(f"   Collection {collection_name} already exists, dropping...")
        drop_collection_if_exists(db, collection_name)
    
    # Create collection with schema validation
    db.create_collection(collection_name, validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['ProductID', 'images', 'detailed_description'],
            'properties': {
                'ProductID': {
                    'bsonType': 'int',
                    'description': 'COMMON IDENTIFIER with SQL Server Product table - REQUIRED'
                },
                'images': {
                    'bsonType': 'array',
                    'minItems': 1,
                    'items': {
                        'bsonType': 'string'
                    },
                    'description': 'Array of image URLs - at least 1 required'
                },
                'detailed_description': {
                    'bsonType': 'string',
                    'minLength': 10,
                    'description': 'Detailed product description - minimum 10 characters'
                },
                'specifications': {
                    'bsonType': 'object',
                    'description': 'Product specs (flexible - varies by category)'
                },
                'nutritional_info': {
                    'bsonType': 'object',
                    'description': 'Nutritional information (only for food products)'
                },
                'availability': {
                    'enum': ['in_stock', 'low_stock', 'out_of_stock'],
                    'description': 'Stock availability status'
                }
            }
        }
    })
    
    print(f"   ✓ Collection {collection_name} created")
    print("   → Schema allows flexible attributes per product category")
    print("   → ProductID is REQUIRED and links to SQL Server")

def create_customer_reviews_collection(db):
    """
    Create Customer_reviews collection
    
    Stores: Customer feedback, ratings, sentiment
    Common Identifiers: ProductID, CustomerID (link to SQL Server)
    """
    print("\n2. Creating Customer_reviews collection...")
    
    collection_name = 'Customer_reviews'
    
    # Drop if exists (for clean setup)
    if collection_exists(db, collection_name):
        print(f"   Collection {collection_name} already exists, dropping...")
        drop_collection_if_exists(db, collection_name)
    
    # Create collection with schema validation
    db.create_collection(collection_name, validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['ProductID', 'CustomerID', 'rating', 'review_text'],
            'properties': {
                'ProductID': {
                    'bsonType': 'int',
                    'description': 'COMMON IDENTIFIER with SQL Server Product table - REQUIRED'
                },
                'CustomerID': {
                    'bsonType': 'int',
                    'description': 'COMMON IDENTIFIER with SQL Server Customer table - REQUIRED'
                },
                'rating': {
                    'bsonType': 'int',
                    'minimum': 1,
                    'maximum': 5,
                    'description': 'Star rating from 1 to 5 - REQUIRED'
                },
                'review_text': {
                    'bsonType': 'string',
                    'minLength': 10,
                    'maxLength': 5000,
                    'description': 'Review text between 10-5000 characters - REQUIRED'
                },
                'sentiment': {
                    'enum': ['positive', 'neutral', 'negative'],
                    'description': 'Sentiment analysis result'
                },
                'sentiment_score': {
                    'bsonType': 'double',
                    'minimum': 0.0,
                    'maximum': 1.0,
                    'description': 'Sentiment confidence score (0.0-1.0)'
                },
                'verified_purchase': {
                    'bsonType': 'bool',
                    'description': 'Whether review is from verified purchase'
                },
                'helpful_votes': {
                    'bsonType': 'int',
                    'minimum': 0,
                    'description': 'Number of helpful votes'
                },
                'reviewer_location': {
                    'bsonType': 'string',
                    'description': 'Reviewer city/location'
                }
            }
        }
    })
    
    print(f"   ✓ Collection {collection_name} created")
    print("   → Schema validates ratings (1-5) and review length (10-5000 chars)")
    print("   → ProductID and CustomerID are REQUIRED and link to SQL Server")

def verify_collections(db):
    """Verify collections were created"""
    print("\n" + "=" * 70)
    print("VERIFICATION: Checking collections...")
    print("=" * 70)
    
    collections = ['Product_catalogue', 'Customer_reviews']
    all_exist = True
    
    for collection_name in collections:
        exists = collection_exists(db, collection_name)
        status = "✓" if exists else "❌"
        print(f"{status} {collection_name}")
        
        if exists:
            # Show validation rules
            coll_info = db.get_collection(collection_name).options()
            if 'validator' in coll_info:
                print(f"   → Has schema validation")
        
        if not exists:
            all_exist = False
    
    print("=" * 70)
    return all_exist

def main():
    """Main execution function"""
    print("=" * 70)
    print("STEP 2.1: CREATE MONGODB COLLECTIONS")
    print("=" * 70)
    print("\nCreating collections with schema validation:")
    print("  • Product_catalogue (flexible product data)")
    print("  • Customer_reviews (user feedback)")
    print("\nCommon Identifiers:")
    print("  • ProductID → Links to SQL Server Product table")
    print("  • CustomerID → Links to SQL Server Customer table")
    print("=" * 70)
    
    try:
        # Connect to MongoDB
        db = get_mongo_connection()
        
        # Create collections
        create_product_catalogue_collection(db)
        create_customer_reviews_collection(db)
        
        # Verify
        if verify_collections(db):
            print("\n✓ SUCCESS! Both collections created with schema validation")
            print("\nKey Features:")
            print("  1. ProductID required in both collections (common identifier)")
            print("  2. CustomerID required in reviews (common identifier)")
            print("  3. Schema validation ensures data quality")
            print("  4. Flexible structure allows category-specific attributes")
            print("\nNext step: Run 02_import_data.py to load JSON data")
        else:
            print("\n❌ Some collections failed to create. Check errors above.")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
