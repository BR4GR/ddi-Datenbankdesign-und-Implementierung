# main.py

from setup.save_to_local_mongo import create_mongo_db
from setup.save_to_local_sql import create_sql_db

def main():
    print("Hello, World!")
    create_mongo_db()
    create_sql_db()


if __name__ == "__main__":
    main()