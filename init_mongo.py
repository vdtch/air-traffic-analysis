from pymongo import MongoClient
from config.mongoconfig import MongoConfig


def get_client():
    config = MongoConfig()
    uri = config.get_mongo_uri()
    client = MongoClient(uri) # Create a client and connect to the server
    client.admin.command('ping') # Send a ping to confirm a successful connection
    print("Pinged your deployment. You successfully connected to MongoDB!")
    return client


def create_collection(client: MongoClient, database_name: str, collection_name: str,):
    try:
        if database_name in client.list_database_names():
            print(f"The database {database_name} already exists.")
        else:
            database = client[database_name]
        
        # Create the collection to insert data
        if collection_name in database.list_collections():
            print(f"The collection {collection_name} already exists.")
        else:
            return database[collection_name]

    except Exception as e:
        raise Exception("Unable to find the document due to the following error: ", e)
