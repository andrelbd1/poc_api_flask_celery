import os
import logging
from typing import Tuple
from pymongo import MongoClient, errors, timeout
from src.config import settings
from datetime import datetime
from bson.objectid import ObjectId
from bson.json_util import dumps
from tqdm import tqdm


class MongoDB:
    def __init__(self) -> None:
        try:
            self.client_mongo = MongoClient(settings.DB_MONGO_URI)
            self.client_mongo.server_info()
        except errors.ServerSelectionTimeoutError as err:
            logging.exception(f"{type(self).__name__} :: Mongo connection timeout: {err!r}")
        except Exception as err:
            logging.exception(f"{type(self).__name__} :: {err!r}")

    def close_connection(self) -> None:
        try:
            self.client_mongo.close()
        except Exception as err:
            logging.exception(f"{type(self).__name__} :: {err!r}")

    def execute_query(self, cursor, query, one=False):
        try:
            with timeout(30):
                if one:
                    return cursor.find_one(query)
                return cursor.find(query)
        except errors.PyMongoError as err:
            if err.timeout:
                logging.exception(f"{type(self).__name__} :: Block timed out: {err!r}")
            else:
                logging.exception(f"{type(self).__name__} :: Failed with non-timeout error: {err!r}")
            return None
        except Exception as err:
            logging.exception(f"{type(self).__name__} :: Failed with error: {err!r}")
            return None

    def select_invoices(self, companyId: str, start_date: datetime = None, end_date: datetime = None):
        db_landing = self.client_mongo['landing']

        query = {}
        query.update({'companyId':{'$eq': companyId}})
        query.update({'processed':{'$eq': True}})

        if start_date and end_date:
            query.update({'date':{'$gte':start_date,'$lte':end_date}})

        logging.info(f"{type(self).__name__} :: {query}")
        return self.execute_query(db_landing.invoices,query)

    def select_product(self, id: str):        
        db_catalog = self.client_mongo['catalog']
        query = {}
        query.update({'_id':ObjectId(id)})
        return self.execute_query(db_catalog.products,query,one=True)

    def select_campaigns(self, ids: list):
        db_service = self.client_mongo['service']
        query = {}
        query.update({'_id':{'$in':[ObjectId(i) for i in ids]}})
        return self.execute_query(db_service.campaigns,query,one=True)