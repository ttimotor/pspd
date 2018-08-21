# !/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2018/8/15 19:19
# @Author  : Mat
# @File    : mongodb.py
# @Software: PyCharm


import pymongo
from config.settings import *


class MongodbBase(object):

    def __init__(self):
        client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
        self.db = client[MONGODB_DATABASE]

    def select(self, table=None, item=None, many='one'):
        collection = self.db[table]
        if many == 'one':
            result = collection.find_one(item)
        elif many == 'any':
            result = collection.find(item)
        else:
            raise Exception('')
        return result

    def insert(self, table=None, data=None):
        collection = self.db[table]
        result = collection.insert(data)
        return result

    def update(self, table=None, item=None, data=None):
        collection = self.db[table]
        document = {"$set": data}
        result = collection.update(item, document)
        return result

    def delete(self, table=None, item=None):
        collection = self.db[table]
        result = collection.remove(item)
        return result


