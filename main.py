# main.py

from setup.save_to_local_mongo import create_mongo_db
from setup.save_to_local_sql import create_sql_db

def main():
    print("Hello, World!")
    print("Setting up databases...")
    print("Creating database MongoDB...")
    create_mongo_db()
    print("Creating database SQL...")
    # create_sql_db()
    print("Databases created successfully!")


if __name__ == "__main__":
    main()