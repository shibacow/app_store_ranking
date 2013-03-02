#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pymongo import Connection
import pymongo
import re,os,logging
from bson.objectid import ObjectId

logdir=os.path.abspath(os.path.dirname(__file__))
logfile='%s/result.log' % logdir

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=logfile,
                    filemode='a')

class MongoOp(object):
    DBNAME='app_store'
    FEED_INFO='feed_info'
    RANKING_INFO='ranking_info'
    FEED_RAW_DATA='feed_raw_data'
    RANKING_RAW_DATA='ranking_raw_data'
    RANKING_META_DATA='ranking_meta_data'
    APP_INFO_DATA='app_info_data'
    def __init__(self,host):
        self.con = Connection(host, 27017)
        self.db=self.con[self.DBNAME]
        self.finfo=self.db[self.FEED_INFO]
        self.rinfo=self.db[self.RANKING_INFO]
        self.frdata=self.db[self.FEED_RAW_DATA]
        self.rrdata=self.db[self.RANKING_RAW_DATA]
        self.rmdata=self.db[self.RANKING_META_DATA]
        self.appdata=self.db[self.APP_INFO_DATA]
    def is_exists(self,col,dicts):
        return self.db[col].find_one(dicts)
    def group(self,col,dicts):
        return self.db[col].group(**dicts)
    def remove(self,col,aid):
        msg='remove aid=%d' % aid
        print msg
        logging.info(msg)
        r=self.db[col].remove({"aid":aid},w=1)
        msg='remvoed aid=%d \t result=%s' % (aid,r)
        print msg
        logging.info(msg)
    def save(self,col,dt):
        if dt:
            self.db[col].save(dt)
    def find_sort_all(self,col,dict1,slist):
        return self.db[col].find(dict1).sort(slist)

    def find_all(self,col,dict1):
        return self.db[col].find(dict1)

def main():
    pass

if __name__=='__main__':main()
