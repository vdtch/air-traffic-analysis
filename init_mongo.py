from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from pymongo.server_api import ServerApi

from config.mongoconfig import MongoConfig


def get_client() -> MongoClient:
    """Create a client and check that the MongoDB deployment is reachable."""
    config = MongoConfig()
    uri = config.get_mongo_uri()

    try:
        client = MongoClient(uri, server_api=ServerApi("1"))
        client.admin.command("ping")  # Send a ping to confirm a successful connection
    except PyMongoError as error:
        raise ConnectionError(f"Unable to connect to MongoDB: {error}") from error

    print("Pinged your deployment. You successfully connected to MongoDB!")
    return client


def create_collection(client: MongoClient, database_name: str, collection_name: str) -> Collection:
    """Return the collection, creating it if it does not exist yet."""
    try:
        database = client[database_name]

        if collection_name in database.list_collection_names():
            print(f"The collection {collection_name} already exists in {database_name}.")
        else:
            database.create_collection(collection_name)
            print(f"The collection {collection_name} has been created in {database_name}.")

        return database[collection_name]

    except PyMongoError as error:
        raise RuntimeError(
            f"Unable to create the collection {collection_name} in {database_name}: {error}"
        ) from error
