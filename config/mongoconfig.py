import os

from dotenv import load_dotenv

# Load MongoDB properties from .env file
load_dotenv()

class MongoConfig:
    def __init__(self) -> None:
        self.mongo_hostname = os.getenv('MONGO_HOSTNAME')
        self.mongo_clustername = os.getenv('MONGO_CLUSTERNAME')
        self.mongo_username = os.getenv('MONGO_USERNAME')
        self.mongo_password = os.getenv('MONGO_PASSWORD')

    
    def get_mongo_uri(self) -> str:
        if self.mongo_username and self.mongo_password:
            return f"mongodb+srv://{self.mongo_username}:{self.mongo_password}@{self.mongo_hostname}/?retryWrites=true&w=majority&appName={self.mongo_clustername}"
        else:
            return f"{self.mongo_hostname}/?retryWrites=true&w=majority&appName={self.mongo_clustername}"