import os

from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB_NAME = "productdb"

# Create a MongoDB client
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]


# Dependency to get the database session for DB operations
def get_mongo_db():
    try:
        yield db
    finally:
        client.close()
