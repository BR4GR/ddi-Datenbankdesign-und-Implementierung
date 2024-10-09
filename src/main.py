import os
import json
from sqlalchemy.orm import Session
from pymongo import MongoClient
from models import Product, NutrientsInformation, Offer, Gtin, Categorie, product_category
from sql_db import SessionLocal
from utils.yeeter import Yeeter

# MongoDB Setup
MONGO_URI = "mongodb://localhost:27017"
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["migros_products"]

# Function to parse and save the product data
def parse_and_save_products():
    yeeter = Yeeter()
    product_dir = "/src/data/products/"
    session = SessionLocal()

    try:
        # Loop over each product file in the directory
        for file_name in os.listdir(product_dir):
            if file_name.endswith(".json"):
                with open(os.path.join(product_dir, file_name), "r") as file:
                    product_data = json.load(file)
                    save_to_mongo(product_data)
                    save_to_sql(product_data, session, yeeter)
    finally:
        session.close()

# Function to save the product data to MongoDB
def save_to_mongo(product_data):
    """Insert the product data into MongoDB"""
    mongo_db.products.insert_one(product_data)
    yeeter.yeet(f"Saved product {product_data['migrosId']} to MongoDB")

# Function to save the product data to SQL
def save_to_sql(product_data, session: Session, yeeter: Yeeter):
    """Insert the product data into the SQL database"""
    
    # Handle Nutrients Information
    nutrients_data = product_data.get("productInformation", {}).get("nutrientsInformation", {}).get("nutrientsTable", {})
    if nutrients_data:
        nutrients = NutrientsInformation(
            energy=nutrients_data.get("rows")[0]["values"][0],
            fat=nutrients_data.get("rows")[1]["values"][0],
            saturates=nutrients_data.get("rows")[2]["values"][0],
            carbohydrate=nutrients_data.get("rows")[3]["values"][0],
            sugars=nutrients_data.get("rows")[4]["values"][0],
            fibre=nutrients_data.get("rows")[5]["values"][0],
            protein=nutrients_data.get("rows")[6]["values"][0],
            salt=nutrients_data.get("rows")[7]["values"][0],
        )
        session.add(nutrients)
        session.commit()  # Commit to generate the ID for nutrients
    else:
        nutrients = None

    # Handle Offer Information
    offer_data = product_data.get("offer", {})
    if offer_data:
        offer = Offer(
            price=offer_data.get("price", {}).get("effectiveValue"),
            quantity=offer_data.get("quantity"),
            unit_price=offer_data.get("price", {}).get("unitPrice", {}).get("value"),
            price_range_min=offer_data.get("price", {}).get("priceRange", {}).get("minPrice"),
            price_range_max=offer_data.get("price", {}).get("priceRange", {}).get("maxPrice")
        )
        session.add(offer)
        session.commit()  # Commit to generate the ID for offer
    else:
        offer = None

    # Handle Gtin Information
    gtins_data = product_data.get("gtins", [])
    if gtins_data:
        gtin = Gtin(
            gtin_number=gtins_data[0]
        )
        session.add(gtin)
        session.commit()  # Commit to generate the ID for gtin
    else:
        gtin = None

    # Handle Categories and Many-to-Many Relationships
    breadcrumb_data = product_data.get("breadcrumb", [])
    categories = []
    for category_data in breadcrumb_data:
        category = session.query(Categorie).filter_by(id=category_data["id"]).first()
        if not category:
            category = Categorie(
                id=category_data["id"],
                name=category_data["name"],
                path="/".join(category_data["slugs"]),
                slug=category_data["slugs"][0],
            )
            session.add(category)
        categories.append(category)
    
    # Finally handle the product
    product = Product(
        migros_id=product_data["migrosId"],
        name=product_data["name"],
        brandLine=product_data.get("brandLine"),
        title=product_data["title"],
        origin=product_data.get("productInformation", {}).get("mainInformation", {}).get("origin"),
        description=product_data.get("description"),
        ingredients=product_data.get("productInformation", {}).get("mainInformation", {}).get("ingredients"),
        nutrient_id=nutrients.id if nutrients else None,
        offer_id=offer.id if offer else None,
        gtin_id=gtin.id if gtin else None
    )
    session.add(product)
    session.commit()

    # Link product with categories
    for category in categories:
        session.execute(product_category.insert().values(product_id=product.migros_id, category_id=category.id))
    session.commit()

    yeeter.yeet(f"Saved product {product_data['migrosId']} to SQL")


if __name__ == "__main__":
    parse_and_save_products()
