#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pymongo import Connection
#from pymongo.objectid import ObjectId
import pymongo
import re

class MongoOp(object):
    DBNAME='app_store'
    FEED_INFO='feed_info'
    RANKING_INFO='ranking_info'
    FEED_RAW_DATA='feed_raw_data'
    RANKING_RAW_DATA='ranking_raw_data'
    def __init__(self,host):
        self.con = Connection(host, 27017)
        self.db=self.con[self.DBNAME]
        self.finfo=self.db[self.FEED_INFO]
        self.rinfo=self.db[self.RANKING_INFO]
        self.frdata=self.db[self.FEED_RAW_DATA]
        self.rrdata=self.db[self.RANKING_RAW_DATA]
    def is_exists(self,col,dicts):
        return self.db[col].find_one(dicts)
    def save(self,col,dt):
        if dt:
            self.db[col].save(dt)

def main():
    pass

if __name__=='__main__':main()
