import json
import os

import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient

from measurements.runner import MeasurementRunner
from setup.save_to_local_mongo import create_mongo_db
from setup.save_to_local_sql import create_sql_db


def check_mongo_db():
    """Check contents of MongoDB database."""
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["productdb"]

        print("\n=== MongoDB Database Status ===")
        print(f"Database: {db.name}")

        collections = db.list_collection_names()
        print(f"Collections: {collections}")

        for collection_name in collections:
            collection = db[collection_name]
            count = collection.count_documents({})
            print(f"- {collection_name}: {count} documents")

            if count > 0:
                sample = collection.find_one()
                print(f"  Sample document:")
                print(json.dumps(sample, indent=2, default=str, ensure_ascii=False))

        client.close()

    except Exception as e:
        print(f"Error checking MongoDB: {e}")


def check_sql_db():
    """Check contents of PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("PG_DB_NAME", "productsandcategories"),
            user=os.getenv("PG_DB_USER", "postgres"),
            password=os.getenv("PG_DB_PASSWORD", "password"),
            host=os.getenv("PG_DB_HOST", "localhost"),
            port=os.getenv("PG_DB_PORT", "5432"),
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        print("\n=== PostgreSQL Database Status ===")

        cursor.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """
        )
        tables = cursor.fetchall()
        print(f"Database: {conn.get_dsn_parameters()['dbname']}")

        if tables:
            table_names = [table["table_name"] for table in tables]
            print(f"Tables: {table_names}")

            for table in tables:
                table_name = table["table_name"]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()["count"]
                print(f"- {table_name}: {count} rows")

                # Show table schema
                if count > 0:
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                    samples = cursor.fetchall()
                    print("   Sample data:")
                    for i, sample in enumerate(samples, 1):
                        print(
                            json.dumps(
                                sample, indent=2, default=str, ensure_ascii=False
                            )
                        )

                print("-" * 60)
        else:
            print("No tables found")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error checking PostgreSQL database: {e}")


def run_measurements():
    """Run database performance measurements."""
    print("\n" + "=" * 60)
    print("RUNNING DATABASE MEASUREMENTS")
    print("=" * 60)

    runner = MeasurementRunner()
    results = runner.run_all_tests(iterations=3)
    report = runner.generate_report(results)
    runner.save_report(report)

    return report


def main():
    limit_products = None  # Set to None for no limit
    print("Setting up databases...")
    print("Creating database MongoDB...")
    create_mongo_db(limit_products=limit_products)
    print("Creating database SQL...")
    create_sql_db(limit_products=limit_products)
    check_sql_db()
    check_mongo_db()

    print("Databases created successfully, hopefully!")
    run_measurements()


if __name__ == "__main__":
    print("Hello, World!")
    main()
    print("Godbye, World!")
