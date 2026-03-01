"""
=====================================================================
WALMART UK POLYGLOT PERSISTENCE - DATA GENERATION SCRIPT
=====================================================================

This script generates realistic UK retail sample data for your Walmart
polyglot persistence project.

GENERATES:
- 7 CSV files for SQL Server tables
- 2 JSON files for MongoDB collections

HOW TO RUN:
1. Install required library: pip install faker
2. Run script: python generate_walmart_data.py
3. Files will be created in the same directory

OUTPUT FILES:
SQL Server CSVs:
  - customers.csv
  - products.csv
  - stores.csv
  - suppliers.csv
  - orders.csv
  - orderlines.csv
  - inventory.csv

MongoDB JSONs:
  - product_catalogue.json
  - customer_reviews.json

"""

import random
import json
import csv
from datetime import datetime, timedelta

# Try to import Faker, provide helpful error if not installed
try:
    from faker import Faker
except ImportError:
    print("ERROR: Faker library not installed.")
    print("Please install it using: pip install faker")
    print("Then run this script again.")
    exit(1)

# Initialize Faker with UK locale for realistic UK data
fake = Faker('en_GB')
Faker.seed(42)  # For reproducibility - same data each time
random.seed(42)

# ============================================================================
# CONFIGURATION - Adjust these numbers if you want more/less data
# ============================================================================

NUM_CUSTOMERS = 1000
NUM_STORES = 20
NUM_SUPPLIERS = 15
NUM_ORDERS = 500
NUM_REVIEWS = 300

# ============================================================================
# UK-SPECIFIC REFERENCE DATA
# ============================================================================

UK_REGIONS = [
    'North West', 'North East', 'Yorkshire', 'West Midlands', 'East Midlands',
    'East of England', 'Greater London', 'South East', 'South West',
    'Wales', 'Scotland', 'Northern Ireland'
]

UK_CITIES = {
    'North West': ['Manchester', 'Liverpool', 'Preston', 'Bolton', 'Warrington'],
    'North East': ['Newcastle', 'Sunderland', 'Middlesbrough', 'Durham'],
    'Yorkshire': ['Leeds', 'Sheffield', 'Bradford', 'York', 'Hull'],
    'West Midlands': ['Birmingham', 'Coventry', 'Wolverhampton', 'Stoke'],
    'East Midlands': ['Nottingham', 'Leicester', 'Derby', 'Northampton'],
    'East of England': ['Norwich', 'Ipswich', 'Cambridge', 'Peterborough'],
    'Greater London': ['London Central', 'Croydon', 'Wembley', 'Stratford'],
    'South East': ['Brighton', 'Oxford', 'Reading', 'Southampton'],
    'South West': ['Bristol', 'Plymouth', 'Exeter', 'Bournemouth'],
    'Wales': ['Cardiff', 'Swansea', 'Newport', 'Wrexham'],
    'Scotland': ['Edinburgh', 'Glasgow', 'Aberdeen', 'Dundee'],
    'Northern Ireland': ['Belfast', 'Derry', 'Lisburn', 'Newry']
}

# Realistic UK grocery and retail products with proper pricing
PRODUCT_CATEGORIES = {
    'Fresh Food': [
        ('Tesco Finest Aberdeen Angus Steak', 12.99, 'FF-STEAK-001', '28-day aged premium beef'),
        ('British Chicken Breast (500g)', 5.49, 'FF-CHICKEN-001', 'Free-range chicken fillets'),
        ('Organic Salmon Fillet (300g)', 8.99, 'FF-SALMON-001', 'Scottish farmed salmon'),
        ('British Pork Chops (4 pack)', 4.99, 'FF-PORK-001', 'Outdoor bred pork'),
        ('Free Range Eggs (12 pack)', 3.49, 'FF-EGGS-001', 'Large British eggs'),
    ],
    'Bakery': [
        ('Warburtons Toastie White Bread', 1.20, 'BK-BREAD-001', 'Soft white sliced bread'),
        ('Hovis Wholemeal Medium Sliced', 1.35, 'BK-BREAD-002', 'Wholegrain goodness'),
        ('Croissants (6 pack)', 2.50, 'BK-CROIS-001', 'All butter pastries'),
        ('Tiger Bloomer Loaf', 1.75, 'BK-BLOOMER-001', 'Crusty white bloomer'),
    ],
    'Grocery': [
        ('Heinz Baked Beans (4x415g)', 2.50, 'GR-BEANS-001', 'Classic in tomato sauce'),
        ("Kellogg's Corn Flakes (750g)", 3.00, 'GR-CEREAL-001', 'Original corn flakes'),
        ('PG Tips Tea Bags (240 pack)', 6.50, 'GR-TEA-001', 'British tea tradition'),
        ('Nescafe Gold Blend (200g)', 8.99, 'GR-COFFEE-001', 'Premium instant coffee'),
        ('Walkers Crisps Variety (24 pack)', 4.50, 'GR-CRISPS-001', 'Assorted flavours'),
        ('Branston Baked Beans (4x410g)', 2.25, 'GR-BEANS-002', 'Rich tomato sauce'),
        ('Hovis Granary Bread Mix', 2.80, 'GR-MIX-001', 'Make your own bread'),
    ],
    'Confectionery': [
        ('Cadbury Dairy Milk (200g)', 2.00, 'CF-CHOC-001', 'Classic milk chocolate'),
        ('Galaxy Smooth Milk (180g)', 2.00, 'CF-CHOC-002', 'Smooth chocolate bar'),
        ('Maltesers (175g)', 1.75, 'CF-CHOC-003', 'Light chocolate treats'),
        ('Quality Street Tin (650g)', 6.00, 'CF-CHOC-004', 'Assorted chocolates'),
        ('Haribo Starmix (140g)', 1.00, 'CF-SWEETS-001', 'Chewy sweets'),
    ],
    'Electronics': [
        ('Samsung 55" QLED TV', 899.00, 'EL-TV-001', '4K Smart Television'),
        ('Dyson V11 Cordless Vacuum', 449.00, 'EL-VAC-001', 'Powerful cordless cleaning'),
        ('Apple iPad 10th Gen', 349.00, 'EL-TABLET-001', '10.9-inch display'),
        ('Sony WH-1000XM5 Headphones', 379.00, 'EL-AUDIO-001', 'Noise cancelling'),
        ('Nintendo Switch OLED', 309.00, 'EL-GAME-001', 'Gaming console'),
    ],
    'Home': [
        ('Hoover Upright Vacuum Cleaner', 149.99, 'HM-VAC-001', 'Bagless cleaning'),
        ('Tefal Non-Stick Pan Set (3pc)', 49.99, 'HM-PAN-001', 'Durable cookware'),
        ('Bosch Series 4 Kettle', 39.99, 'HM-KETTLE-001', '1.7L rapid boil'),
        ('Brabantia Ironing Board', 89.99, 'HM-IRON-001', 'Sturdy ironing solution'),
        ('Vileda SuperMocio Mop Set', 24.99, 'HM-MOP-001', 'Easy floor cleaning'),
    ],
    'Toys': [
        ('LEGO Star Wars Millennium Falcon', 149.99, 'TY-LEGO-001', 'Iconic starship set'),
        ('Barbie Dreamhouse Playset', 199.99, 'TY-BARBIE-001', 'Ultimate dollhouse'),
        ('Hot Wheels Track Builder', 29.99, 'TY-HWHEELS-001', 'Stunt track system'),
        ('Nerf Elite 2.0 Blaster', 24.99, 'TY-NERF-001', 'Foam dart blaster'),
        ('Play-Doh Mega Pack (36 cans)', 19.99, 'TY-PLAYDOH-001', 'Creative modelling'),
    ],
    'Clothing': [
        ('M&S Cotton Formal Shirt', 29.99, 'CL-SHIRT-001', 'Pure cotton dress shirt'),
        ('Next Slim Fit Jeans', 39.99, 'CL-JEANS-001', 'Modern denim fit'),
        ('John Lewis Cashmere Jumper', 79.99, 'CL-JUMPER-001', 'Luxury knitwear'),
        ('F&F School Uniform Pack', 24.99, 'CL-UNIFORM-001', 'Complete school outfit'),
        ('Nike Air Max Trainers', 119.99, 'CL-SHOES-001', 'Classic sports shoes'),
    ],
}

# Realistic UK supplier names
UK_SUPPLIER_NAMES = [
    'ABP Food Group', 'Warburtons Ltd', 'Kraft Heinz UK', 'Mondelez UK',
    'Samsung Electronics UK', 'Dyson Ltd', 'Apple UK', 'Sony UK',
    'Unilever UK', 'Nestle UK', 'PepsiCo UK', 'Mars Wrigley UK',
    'Tesco Grocery Wholesale', 'Booker Wholesale', 'Brakes Group'
]

# Review templates for generating realistic customer feedback
REVIEW_TEMPLATES = {
    5: [
        "Excellent quality {product}, {positive_aspect}. Highly recommend!",
        "Absolutely brilliant {product}. {positive_aspect}. Will buy again.",
        "Best {product} I've purchased. {positive_aspect}. Five stars!",
        "Outstanding value. {positive_aspect}. Can't fault it.",
    ],
    4: [
        "Very good {product}. {positive_aspect}, though {minor_issue}.",
        "Really pleased with this {product}. {positive_aspect} but {minor_issue}.",
        "Solid product. {positive_aspect}. Only minor issue is {minor_issue}.",
    ],
    3: [
        "Decent {product}. {positive_aspect} but {negative_aspect}.",
        "It's OK. {positive_aspect} however {negative_aspect}.",
        "Average quality. {mixed_feeling}. Does the job.",
    ],
    2: [
        "Not impressed. {negative_aspect}. Expected better for the price.",
        "Disappointed with this {product}. {negative_aspect}.",
        "Below average. {negative_aspect}. Wouldn't recommend.",
    ],
    1: [
        "Terrible {product}. {negative_aspect}. Waste of money.",
        "Very poor quality. {negative_aspect}. Returning it.",
        "Awful. {negative_aspect}. Do not buy.",
    ]
}

REVIEW_ASPECTS = {
    'positive': [
        'great value for money', 'excellent quality', 'fresh and tasty',
        'arrived on time', 'packaging was perfect', 'family loved it',
        'lasts a long time', 'perfect for Sunday roast', 'kids enjoyed it',
        'better than branded alternatives', 'good portion size'
    ],
    'negative': [
        'arrived damaged', 'past its use-by date', 'poor quality',
        'not worth the price', 'smaller than expected', 'packaging was poor',
        "didn't taste fresh", 'too expensive', 'misleading description'
    ],
    'minor': [
        'packaging could be improved', 'slightly smaller than expected',
        'wish it came in larger size', 'delivery took 2 days'
    ],
    'mixed': [
        'OK for the price but nothing special', 'meets basic expectations',
        'average compared to competitors'
    ]
}

# ============================================================================
# DATA GENERATION FUNCTIONS
# ============================================================================

def generate_customers(n=NUM_CUSTOMERS):
    """Generate realistic UK customer data"""
    print(f"Generating {n} customers...")
    customers = []
    
    for i in range(1, n + 1):
        region = random.choice(list(UK_CITIES.keys()))
        city = random.choice(UK_CITIES[region])
        
        customer = {
            'CustomerID': i,
            'CustomerName': fake.name(),
            'Email': fake.email(),
            'Phone': fake.phone_number(),
            'Address': fake.street_address(),
            'City': city,
            'Postcode': fake.postcode(),
            'RegistrationDate': fake.date_between(start_date='-3y', end_date='today'),
            'IsActive': 1
        }
        customers.append(customer)
    
    return customers

def generate_products():
    """Generate realistic UK product data from predefined catalog"""
    print("Generating products from UK retail catalog...")
    products = []
    product_id = 1
    
    for category, items in PRODUCT_CATEGORIES.items():
        for name, price, sku, description in items:
            product = {
                'ProductID': product_id,
                'ProductName': name,
                'Category': category,
                'Price': price,
                'SKU': sku,
                'Description': description,
                'IsActive': 1,
                'CreatedDate': fake.date_between(start_date='-2y', end_date='today')
            }
            products.append(product)
            product_id += 1
    
    return products

def generate_stores(n=NUM_STORES):
    """Generate realistic UK store locations"""
    print(f"Generating {n} stores across UK regions...")
    stores = []
    store_id = 1
    
    # Distribute stores across different regions
    for region in random.sample(list(UK_CITIES.keys()), min(n, len(UK_CITIES))):
        cities = UK_CITIES[region]
        for city in random.sample(cities, min(2, len(cities))):
            if store_id > n:
                break
            
            store = {
                'StoreID': store_id,
                'StoreName': f'Walmart Supercentre {city}',
                'Location': f'{fake.street_name()} Shopping Park',
                'Region': region,
                'ManagerName': fake.name(),
                'Phone': fake.phone_number(),
                'OpeningDate': fake.date_between(start_date='-10y', end_date='-1y'),
                'IsActive': 1
            }
            stores.append(store)
            store_id += 1
    
    return stores[:n]

def generate_suppliers(n=NUM_SUPPLIERS):
    """Generate realistic UK supplier data"""
    print(f"Generating {n} suppliers...")
    suppliers = []
    
    for i, company in enumerate(random.sample(UK_SUPPLIER_NAMES, n), 1):
        # Generate realistic email domain from company name
        domain = company.lower().replace(' ', '').replace('ltd', '').replace('&', '')
        
        supplier = {
            'SupplierID': i,
            'CompanyName': company,
            'ContactName': fake.name(),
            'ContactEmail': f"{fake.user_name()}@{domain}.co.uk",
            'Phone': fake.phone_number(),
            'Address': fake.street_address(),
            'City': random.choice([city for cities in UK_CITIES.values() for city in cities]),
            'Country': 'United Kingdom',
            'IsActive': 1
        }
        suppliers.append(supplier)
    
    return suppliers

def generate_orders_and_orderlines(customers, products, stores, n_orders=NUM_ORDERS):
    """Generate realistic shopping orders with UK basket patterns"""
    print(f"Generating {n_orders} orders with realistic basket compositions...")
    orders = []
    orderlines = []
    orderline_id = 1
    
    # Realistic UK shopping basket patterns
    basket_patterns = [
        ['Fresh Food', 'Bakery', 'Grocery'],  # Weekly food shop
        ['Fresh Food', 'Confectionery'],  # Quick meal + treat
        ['Electronics'],  # Big ticket purchase
        ['Grocery', 'Confectionery', 'Bakery'],  # Pantry stock-up
        ['Clothing'],  # Fashion shopping
        ['Toys'],  # Gift buying
        ['Home'],  # Home improvement
        ['Fresh Food', 'Bakery'],  # Daily essentials
    ]
    
    for order_id in range(1, n_orders + 1):
        customer = random.choice(customers)
        store = random.choice(stores)
        order_date = fake.date_time_between(start_date='-3m', end_date='now')
        
        # Select shopping pattern
        pattern = random.choice(basket_patterns)
        
        # Get products matching this pattern
        pattern_products = [p for p in products if p['Category'] in pattern]
        num_items = random.randint(1, min(8, len(pattern_products)))
        order_products = random.sample(pattern_products, num_items)
        
        # Create order lines and calculate total
        total_amount = 0
        for product in order_products:
            # Electronics usually bought in quantity 1, food in 1-3
            quantity = random.randint(1, 3) if product['Category'] != 'Electronics' else 1
            unit_price = product['Price']
            line_total = quantity * unit_price
            total_amount += line_total
            
            orderline = {
                'OrderLineID': orderline_id,
                'OrderID': order_id,
                'ProductID': product['ProductID'],
                'Quantity': quantity,
                'UnitPrice': unit_price,
                'LineTotal': round(line_total, 2)
            }
            orderlines.append(orderline)
            orderline_id += 1
        
        # Create order
        order = {
            'OrderID': order_id,
            'CustomerID': customer['CustomerID'],
            'StoreID': store['StoreID'],
            'OrderDate': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'TotalAmount': round(total_amount, 2),
            'OrderStatus': random.choices(
                ['Completed', 'Processing', 'Cancelled'],
                weights=[0.85, 0.10, 0.05]  # Most orders completed
            )[0],
            'PaymentMethod': random.choice([
                'Contactless Card', 'Chip & PIN', 'Mobile Payment', 'Cash'
            ])
        }
        orders.append(order)
    
    return orders, orderlines

def generate_inventory(products, stores, suppliers):
    """Generate realistic inventory stock levels"""
    print("Generating inventory records (store-product-supplier combinations)...")
    inventory = []
    inventory_id = 1
    
    for store in stores:
        # Each store stocks 60-80% of available products
        num_products = random.randint(int(len(products) * 0.6), int(len(products) * 0.8))
        store_products = random.sample(products, num_products)
        
        for product in store_products:
            supplier = random.choice(suppliers)
            
            # Stock levels vary realistically by category
            if product['Category'] == 'Electronics':
                stock_qty = random.randint(5, 25)
                reorder_level = 5
                max_stock = 50
            elif product['Category'] in ['Fresh Food', 'Bakery']:
                stock_qty = random.randint(50, 300)
                reorder_level = 50
                max_stock = 500
            else:  # Grocery, Confectionery, etc.
                stock_qty = random.randint(100, 500)
                reorder_level = 100
                max_stock = 1000
            
            inventory_item = {
                'InventoryID': inventory_id,
                'ProductID': product['ProductID'],
                'StoreID': store['StoreID'],
                'SupplierID': supplier['SupplierID'],
                'StockQuantity': stock_qty,
                'ReorderLevel': reorder_level,
                'MaxStockLevel': max_stock,
                'LastRestocked': fake.date_between(start_date='-30d', end_date='today').strftime('%Y-%m-%d'),
                'CostPrice': round(product['Price'] * random.uniform(0.55, 0.75), 2)  # Cost is 55-75% of retail
            }
            inventory.append(inventory_item)
            inventory_id += 1
    
    return inventory

def generate_product_catalogue_mongo(products):
    """Generate MongoDB Product_catalogue collection documents"""
    print("Generating MongoDB Product Catalogue documents...")
    catalogue = []
    
    # Nutritional info templates for food categories
    nutritional_templates = {
        'Fresh Food': {
            'calories_per_100g': lambda: random.randint(150, 300),
            'protein': lambda: f"{random.randint(15, 30)}g",
            'fat': lambda: f"{random.randint(5, 20)}g",
            'carbohydrates': lambda: f"{random.randint(0, 5)}g"
        },
        'Bakery': {
            'calories_per_100g': lambda: random.randint(220, 280),
            'protein': lambda: f"{random.randint(7, 12)}g",
            'fat': lambda: f"{random.randint(2, 8)}g",
            'carbohydrates': lambda: f"{random.randint(40, 55)}g"
        },
        'Grocery': {
            'calories_per_100g': lambda: random.randint(300, 400),
            'protein': lambda: f"{random.randint(5, 15)}g",
            'fat': lambda: f"{random.randint(1, 10)}g",
            'carbohydrates': lambda: f"{random.randint(60, 80)}g"
        }
    }
    
    for product in products:
        doc = {
            'ProductID': product['ProductID'],  # COMMON IDENTIFIER with SQL Server
            'images': [
                f"https://cdn.walmart.co.uk/products/{product['SKU']}_main.jpg",
                f"https://cdn.walmart.co.uk/products/{product['SKU']}_detail.jpg",
                f"https://cdn.walmart.co.uk/products/{product['SKU']}_nutrition.jpg"
            ],
            'detailed_description': f"{product['Description']}. {fake.text(max_nb_chars=100)}",
            'specifications': {
                'brand': product['ProductName'].split()[0],
                'origin': random.choice(['UK', 'Scotland', 'British', 'European']),
            },
            'availability': random.choice(['in_stock', 'low_stock', 'out_of_stock']),
            'last_updated': datetime.now().isoformat()
        }
        
        # Add nutritional info for food products
        if product['Category'] in nutritional_templates:
            template = nutritional_templates[product['Category']]
            doc['nutritional_info'] = {
                key: func() for key, func in template.items()
            }
        
        catalogue.append(doc)
    
    return catalogue

def generate_customer_reviews_mongo(orderlines, orders, customers, products, n=NUM_REVIEWS):
    """Generate MongoDB Customer_reviews collection documents"""
    print(f"Generating {n} MongoDB Customer Reviews...")
    reviews = []
    
    # Only create reviews for completed orders
    eligible_orderlines = random.sample(orderlines, min(n, len(orderlines)))
    
    for orderline in eligible_orderlines:
        # Find the order this belongs to
        order = next((o for o in orders if o['OrderID'] == orderline['OrderID']), None)
        if not order or order['OrderStatus'] != 'Completed':
            continue
        
        customer = next((c for c in customers if c['CustomerID'] == order['CustomerID']), None)
        product = next((p for p in products if p['ProductID'] == orderline['ProductID']), None)
        
        if not customer or not product:
            continue
        
        # Rating distribution (skewed towards positive as is realistic)
        rating = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.10, 0.15, 0.30, 0.40])[0]
        
        # Generate realistic review text
        template = random.choice(REVIEW_TEMPLATES[rating])
        
        if rating >= 4:
            positive = random.choice(REVIEW_ASPECTS['positive'])
            minor = random.choice(REVIEW_ASPECTS['minor'])
            review_text = template.format(
                product=product['ProductName'],
                positive_aspect=positive,
                minor_issue=minor
            )
        elif rating == 3:
            positive = random.choice(REVIEW_ASPECTS['positive'])
            negative = random.choice(REVIEW_ASPECTS['negative'])
            mixed = random.choice(REVIEW_ASPECTS['mixed'])
            review_text = template.format(
                product=product['ProductName'],
                positive_aspect=positive,
                negative_aspect=negative,
                mixed_feeling=mixed
            )
        else:  # Rating 1-2
            negative = random.choice(REVIEW_ASPECTS['negative'])
            review_text = template.format(
                product=product['ProductName'],
                negative_aspect=negative
            )
        
        review = {
            'ProductID': product['ProductID'],  # COMMON IDENTIFIER with SQL Server
            'CustomerID': customer['CustomerID'],  # COMMON IDENTIFIER with SQL Server
            'rating': rating,
            'review_text': review_text,
            'review_date': fake.date_time_between(start_date='-2m', end_date='now').isoformat(),
            'verified_purchase': True,
            'helpful_votes': random.randint(0, 50),
            'not_helpful_votes': random.randint(0, 5),
            'reviewer_location': customer['City'],
            'reviewer_age_group': random.choice(['18-24', '25-34', '35-44', '45-54', '55-64', '65+']),
            'sentiment': 'positive' if rating >= 4 else ('neutral' if rating == 3 else 'negative'),
            'sentiment_score': round(rating / 5.0, 2),
            'moderator_approved': True,
            'moderation_date': fake.date_time_between(start_date='-1m', end_date='now').isoformat()
        }
        reviews.append(review)
    
    return reviews

def save_csv(filename, data, fieldnames):
    """Save data to CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Saved {filename}")

def save_json(filename, data):
    """Save data to JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)
    print(f"✓ Saved {filename}")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("=" * 70)
    print("WALMART UK POLYGLOT PERSISTENCE - DATA GENERATOR")
    print("=" * 70)
    print()
    
    # Generate all data
    print("STEP 1: Generating data...")
    print("-" * 70)
    
    customers = generate_customers()
    products = generate_products()
    stores = generate_stores()
    suppliers = generate_suppliers()
    orders, orderlines = generate_orders_and_orderlines(customers, products, stores)
    inventory = generate_inventory(products, stores, suppliers)
    product_catalogue = generate_product_catalogue_mongo(products)
    customer_reviews = generate_customer_reviews_mongo(orderlines, orders, customers, products)
    
    print()
    print("=" * 70)
    print("STEP 2: Saving to files...")
    print("-" * 70)
    
    # Save SQL Server CSV files
    save_csv('customers.csv', customers, customers[0].keys())
    save_csv('products.csv', products, products[0].keys())
    save_csv('stores.csv', stores, stores[0].keys())
    save_csv('suppliers.csv', suppliers, suppliers[0].keys())
    save_csv('orders.csv', orders, orders[0].keys())
    save_csv('orderlines.csv', orderlines, orderlines[0].keys())
    save_csv('inventory.csv', inventory, inventory[0].keys())
    
    # Save MongoDB JSON files
    save_json('product_catalogue.json', product_catalogue)
    save_json('customer_reviews.json', customer_reviews)
    
    # Print summary statistics
    print()
    print("=" * 70)
    print("SUMMARY STATISTICS")
    print("=" * 70)
    print(f"Customers:           {len(customers):,}")
    print(f"Products:            {len(products):,}")
    print(f"Stores:              {len(stores):,}")
    print(f"Suppliers:           {len(suppliers):,}")
    print(f"Orders:              {len(orders):,}")
    print(f"Order Lines:         {len(orderlines):,}")
    print(f"Inventory Records:   {len(inventory):,}")
    print(f"Product Catalogue:   {len(product_catalogue):,}")
    print(f"Customer Reviews:    {len(customer_reviews):,}")
    print()
    print(f"Avg items per order: {len(orderlines) / len(orders):.1f}")
    print(f"Total revenue:       £{sum(o['TotalAmount'] for o in orders):,.2f}")
    
    # Show sample records
    print()
    print("=" * 70)
    print("SAMPLE RECORDS")
    print("=" * 70)
    print("\nSample Customer:")
    print(json.dumps(customers[0], indent=2, default=str))
    
    print("\nSample Product:")
    print(json.dumps(products[0], indent=2, default=str))
    
    print("\nSample Order:")
    print(json.dumps(orders[0], indent=2, default=str))
    
    print("\nSample MongoDB Product Catalogue:")
    print(json.dumps(product_catalogue[0], indent=2, default=str))
    
    print("\nSample MongoDB Customer Review:")
    print(json.dumps(customer_reviews[0], indent=2, default=str))
    
    print()
    print("=" * 70)
    print("✓ SUCCESS! All files generated in current directory")
    print("=" * 70)
    print()
    print("NEXT STEPS:")
    print("1. Import CSV files to SQL Server using SQL Server Import Wizard")
    print("2. Import JSON files to MongoDB using mongoimport:")
    print("   mongoimport --db walmart --collection Product_catalogue \\")
    print("               --file product_catalogue.json --jsonArray")
    print("   mongoimport --db walmart --collection Customer_reviews \\")
    print("               --file customer_reviews.json --jsonArray")
    print()

if __name__ == "__main__":
    main()
