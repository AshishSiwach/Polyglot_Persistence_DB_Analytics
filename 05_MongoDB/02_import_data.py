"""
STEP 2.2: Import Data to MongoDB
Walmart UK Polyglot Persistence Project

Imports JSON files generated from generate_walmart_data.py
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent / '01_Setup'))

from mongo_utils import get_mongo_connection, insert_documents, load_json_file, get_document_count
from config import JSON_FILES

def import_product_catalogue(db):
    """Import Product_catalogue from JSON"""
    print("\n1. Importing Product_catalogue...")
    
    json_path = JSON_FILES['product_catalogue']
    if not json_path.exists():
        print(f"   ❌ File not found: {json_path}")
        return 0
    
    # Load JSON data
    print(f"   Loading {json_path.name}...")
    documents = load_json_file(json_path)
    
    print(f"   Found {len(documents)} documents")
    
    # Insert documents
    count = insert_documents(db, 'Product_catalogue', documents)
    print(f"   → {count} product catalogue entries imported")
    print("   → Each ProductID matches SQL Server Product table")
    
    return count

def import_customer_reviews(db):
    """Import Customer_reviews from JSON"""
    print("\n2. Importing Customer_reviews...")
    
    json_path = JSON_FILES['customer_reviews']
    if not json_path.exists():
        print(f"   ❌ File not found: {json_path}")
        return 0
    
    # Load JSON data
    print(f"   Loading {json_path.name}...")
    documents = load_json_file(json_path)
    
    print(f"   Found {len(documents)} documents")
    
    # Insert documents
    count = insert_documents(db, 'Customer_reviews', documents)
    print(f"   → {count} customer reviews imported")
    print("   → ProductID and CustomerID match SQL Server")
    
    return count

def verify_import(db):
    """Verify data was imported correctly"""
    print("\n" + "=" * 70)
    print("VERIFICATION: Document Counts")
    print("=" * 70)
    
    collections = {
        'Product_catalogue': 'product_catalogue',
        'Customer_reviews': 'customer_reviews'
    }
    
    all_correct = True
    
    for collection_name, json_key in collections.items():
        db_count = get_document_count(db, collection_name)
        
        # Count JSON documents
        json_path = JSON_FILES[json_key]
        if json_path.exists():
            json_data = load_json_file(json_path)
            json_count = len(json_data)
        else:
            json_count = 0
        
        match = "✓" if db_count == json_count else "❌"
        print(f"{match} {collection_name:25s} - DB: {db_count:>6,} | JSON: {json_count:>6,}")
        
        if db_count != json_count:
            all_correct = False
    
    print("=" * 70)
    return all_correct

def main():
    """Main execution function"""
    print("=" * 70)
    print("STEP 2.2: IMPORT DATA TO MONGODB")
    print("=" * 70)
    print(f"\nData directory: {JSON_FILES['product_catalogue'].parent}")
    print("\nNote: Ensure you've run generate_walmart_data.py first!")
    print("=" * 70)
    
    try:
        # Connect to MongoDB
        db = get_mongo_connection()
        
        # Import all data
        import_product_catalogue(db)
        import_customer_reviews(db)
        
        # Verify
        if verify_import(db):
            print("\n✓ SUCCESS! All data imported correctly")
            print("\nCommon Identifiers Loaded:")
            print("  • ProductID in Product_catalogue → matches SQL Server")
            print("  • ProductID in Customer_reviews → matches SQL Server")
            print("  • CustomerID in Customer_reviews → matches SQL Server")
            print("\nNext step: Run 03_create_indexes.py to optimize queries")
        else:
            print("\n⚠ WARNING: Document count mismatch detected")
            print("Check JSON files and MongoDB collections")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
