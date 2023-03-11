import pandas as pd
from pymongo import ASCENDING, MongoClient, ReplaceOne
from pymongo.database import Database
from pymongo.cursor import Cursor
from pymongo.collection import Collection
from pymongo.results import DeleteResult


class MongodbDatabase:

    def __init__(self, db_name):
        mongo_client = MongoClient(host="localhost", port=27017)

        self.db = mongo_client[db_name]

    def get_collections(self):
        return self.db.list_collection_names()

    def save_tick_data(self):
        pass

    def load_tick_data(self, collection_name, query):

        cursor = self.db[collection_name].find(query)

        df = pd.DataFrame(list(cursor))

        return df
