import os
import json
import psycopg2

# Set paths to load data
PRODUCTS_PATH = "src/data/product/"
CATEGORIES_PATH = "src/data/categorie/"


# Connect to the PostgreSQL database
def connect_to_postgres():
    """Connect to the local PostgreSQL database."""
    return psycopg2.connect(
        dbname="productdb",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )


# Load JSON files
def load_documents_from_folder(folder_path):
    """Load JSON documents from the specified folder."""
    documents = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            with open(os.path.join(folder_path, filename), "r") as f:
                documents.append(json.load(f))
    return documents


# Insert category data into PostgreSQL
def insert_category_data(cursor, category_documents):
    """Insert category data into PostgreSQL."""
    for category in category_documents:
        cursor.execute(
            """
            INSERT INTO category (id, name, path, slug)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """,
            (
                category["id"],
                category["name"],
                category.get("path"),
                category.get("slug"),
            ),
        )


# Insert nutrients data into PostgreSQL
def insert_nutrients(cursor, nutrients):
    """Insert nutrients data into PostgreSQL and return the nutrient ID."""
    cursor.execute(
        """
        INSERT INTO nutrients (unit, quantity, energy, fat, saturates, carbohydrate, sugars, fibre, protein, salt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """,
        (
            "g",  # Assuming unit
            len(nutrients),  # Quantity can be derived or set based on your requirement
            nutrients[0]["values"][
                0
            ],  # This assumes data is consistent; adjust as needed
            nutrients[1]["values"][0],
            nutrients[2]["values"][0],
            nutrients[3]["values"][0],
            nutrients[4]["values"][0],
            nutrients[5]["values"][0],
            nutrients[6]["values"][0],
            nutrients[7]["values"][0],
        ),
    )
    return cursor.fetchone()[0]


# Insert offer data into PostgreSQL
def insert_offer(cursor, offer):
    """Insert offer data into PostgreSQL and return the offer ID."""
    price = offer["price"]["effectiveValue"]
    price_range_min = offer["price"]["priceRange"]["minPrice"]
    price_range_max = offer["price"]["priceRange"]["maxPrice"]
    quantity = offer.get("quantity")
    unit_price = offer["price"]["unitPrice"]["value"]

    cursor.execute(
        """
        INSERT INTO offer (price, quantity, unit_price, price_range_min, price_range_max)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """,
        (price, quantity, unit_price, price_range_min, price_range_max),
    )
    return cursor.fetchone()[0]


# Insert GTIN data into PostgreSQL
def insert_gtins(cursor, gtins, product_id):
    """Insert GTINs into PostgreSQL and update the product with GTIN ID."""
    for gtin in gtins:
        cursor.execute(
            """
            INSERT INTO gtin (gtin_number)
            VALUES (%s)
            RETURNING id
        """,
            (gtin,),
        )
        gtin_id = cursor.fetchone()[0]

        # Update product table with GTIN id
        cursor.execute(
            """
            UPDATE product SET gtin_id = %s WHERE migros_id = %s
        """,
            (gtin_id, product_id),
        )


# Insert product-category relationships into PostgreSQL
def insert_product_category_relationships(cursor, breadcrumbs, product_id):
    """Insert product-category relationships into PostgreSQL."""
    for breadcrumb in breadcrumbs:
        cursor.execute(
            """
            INSERT INTO product_category (product_id, category_id)
            VALUES (%s, %s)
        """,
            (product_id, breadcrumb["id"]),
        )


# Insert product data into PostgreSQL
def insert_product_data(cursor, product_documents):
    """Insert product data into PostgreSQL."""
    for product in product_documents:
        # Insert nutrients if available
        nutrients = product["productInformation"]["nutrientsInformation"][
            "nutrientsTable"
        ]["rows"]
        nutrient_id = None
        if nutrients:
            nutrient_id = insert_nutrients(cursor, nutrients)

        # Insert offer if available
        offer = product.get("offer")
        offer_id = None
        if offer:
            offer_id = insert_offer(cursor, offer)

        # Insert product
        cursor.execute(
            """
            INSERT INTO product (migros_id, name, brand_line, title, origin, description, ingredients, nutrient_id, offer_id, gtin_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
        """,
            (
                product["migrosId"],
                product["name"],
                product.get("brandLine"),
                product.get("title"),
                product["productInformation"]["mainInformation"].get("origin"),
                product.get("description"),
                product["productInformation"]["mainInformation"].get("ingredients"),
                nutrient_id,
                offer_id,
            ),
        )

        # Insert GTINs
        gtins = product.get("gtins")
        if gtins:
            insert_gtins(cursor, gtins, product["migrosId"])

        # Insert product-category relationships
        breadcrumbs = product.get("breadcrumb")
        if breadcrumbs:
            insert_product_category_relationships(
                cursor, breadcrumbs, product["migrosId"]
            )


# Save data to PostgreSQL
def save_data_to_postgres():
    """Save JSON files from local folder to PostgreSQL."""
    connection = connect_to_postgres()
    cursor = connection.cursor()

    product_documents = load_documents_from_folder(PRODUCTS_PATH)
    category_documents = load_documents_from_folder(CATEGORIES_PATH)

    insert_category_data(cursor, category_documents)
    insert_product_data(cursor, product_documents)

    # Commit changes
    connection.commit()

    # Close connection
    cursor.close()
    connection.close()


def main():
    save_data_to_postgres()


if __name__ == "__main__":
    main()
