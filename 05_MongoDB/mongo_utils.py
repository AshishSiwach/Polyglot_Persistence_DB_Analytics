"""
MongoDB Utility Functions
Helper functions for MongoDB operations
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import sys
from pathlib import Path
import json

# Add setup directory to path
sys.path.append(str(Path(__file__).parent.parent / '01_Setup'))
from config import MONGO_CONFIG, MONGO_CONNECTION_STRING

def get_mongo_connection():
    """
    Create and return MongoDB connection
    
    Returns:
        Database: MongoDB database object
    """
    try:
        client = MongoClient(
            MONGO_CONNECTION_STRING,
            serverSelectionTimeoutMS=5000  # 5 second timeout
        )
        
        # Test connection
        client.admin.command('ping')
        
        db = client[MONGO_CONFIG['database']]
        print(f"✓ Connected to MongoDB: {MONGO_CONFIG['database']}")
        return db
    
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        print("\nTroubleshooting:")
        print("1. Check if MongoDB is running:")
        print("   Windows: net start MongoDB")
        print("   Mac: brew services start mongodb-community")
        print("   Linux: sudo systemctl start mongod")
        print("2. Verify host/port in config.py")
        print("3. Check firewall settings")
        raise

def collection_exists(db, collection_name):
    """
    Check if collection exists
    
    Args:
        db: Database connection
        collection_name (str): Name of collection
    
    Returns:
        bool: True if collection exists
    """
    return collection_name in db.list_collection_names()

def get_document_count(db, collection_name):
    """
    Get number of documents in collection
    
    Args:
        db: Database connection
        collection_name (str): Name of collection
    
    Returns:
        int: Number of documents
    """
    return db[collection_name].count_documents({})

def drop_collection_if_exists(db, collection_name):
    """
    Drop collection if it exists
    
    Args:
        db: Database connection
        collection_name (str): Name of collection to drop
    """
    if collection_exists(db, collection_name):
        db[collection_name].drop()
        print(f"✓ Dropped collection {collection_name}")

def insert_documents(db, collection_name, documents, batch_size=1000):
    """
    Insert multiple documents with batching
    
    Args:
        db: Database connection
        collection_name (str): Collection name
        documents (list): List of document dictionaries
        batch_size (int): Documents per batch
    
    Returns:
        int: Number of documents inserted
    """
    collection = db[collection_name]
    total_inserted = 0
    
    try:
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            result = collection.insert_many(batch, ordered=False)
            total_inserted += len(result.inserted_ids)
            print(f"  Inserted {total_inserted}/{len(documents)} documents...", end='\r')
        
        print(f"\n✓ Inserted {total_inserted} documents to {collection_name}")
        return total_inserted
    
    except Exception as e:
        print(f"\n❌ Error inserting documents: {e}")
        raise

def create_index(db, collection_name, index_spec, **kwargs):
    """
    Create index on collection
    
    Args:
        db: Database connection
        collection_name (str): Collection name
        index_spec: Index specification (e.g., "field_name" or [("field", 1)])
        **kwargs: Additional index options (unique, name, etc.)
    
    Returns:
        str: Index name
    """
    collection = db[collection_name]
    
    try:
        index_name = collection.create_index(index_spec, **kwargs)
        print(f"✓ Created index on {collection_name}: {index_name}")
        return index_name
    
    except Exception as e:
        print(f"❌ Error creating index: {e}")
        raise

def print_documents(cursor, max_docs=5):
    """
    Pretty print documents from cursor
    
    Args:
        cursor: MongoDB cursor
        max_docs (int): Maximum documents to display
    """
    documents = list(cursor)
    
    if not documents:
        print("(No documents)")
        return
    
    for i, doc in enumerate(documents[:max_docs], 1):
        print(f"\n--- Document {i} ---")
        print(json.dumps(doc, indent=2, default=str))
    
    if len(documents) > max_docs:
        print(f"\n... ({len(documents) - max_docs} more documents)")
    
    print(f"\nTotal documents: {len(documents)}")

def verify_mongodb_setup(db):
    """
    Verify collections exist and have data
    
    Args:
        db: Database connection
    
    Returns:
        dict: Collection names and document counts
    """
    collections = ['Product_catalogue', 'Customer_reviews']
    results = {}
    
    print("\nMongoDB Verification:")
    print("=" * 50)
    
    for collection_name in collections:
        if collection_exists(db, collection_name):
            count = get_document_count(db, collection_name)
            results[collection_name] = count
            status = "✓" if count > 0 else "⚠"
            print(f"{status} {collection_name:25s} {count:>10,} documents")
        else:
            results[collection_name] = 0
            print(f"❌ {collection_name:25s} NOT FOUND")
    
    print("=" * 50)
    return results

def load_json_file(filepath):
    """
    Load JSON data from file
    
    Args:
        filepath (Path or str): Path to JSON file
    
    Returns:
        list or dict: Parsed JSON data
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

# Test connection when run directly
if __name__ == "__main__":
    print("Testing MongoDB connection...")
    
    try:
        db = get_mongo_connection()
        print("\n✓ Connection successful!")
        
        # Test query
        server_info = db.client.server_info()
        print(f"\nMongoDB Version: {server_info['version']}")
        
        # List collections
        collections = db.list_collection_names()
        if collections:
            print(f"\nExisting collections: {', '.join(collections)}")
        else:
            print("\nNo collections found (database is empty)")
        
        print("\n✓ Connection test complete")
        
    except Exception as e:
        print(f"\n❌ Connection test failed: {e}")
