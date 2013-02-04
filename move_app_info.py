#!/usr/bin/python
# -*- coding:utf-8 -*-
import requests
import simplejson
import re
from datetime import datetime
import logging
import os
#import time
import selector_info
import mog_op
import pprint
import hashlib
import pymongo

logdir=os.path.abspath(os.path.dirname(__file__))
logfile='%s/result.log' % logdir

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=logfile,
                    filemode='a')

def save_raw_data(mp,fd):
    fd['created_at']=datetime.now()
    mp.save(mp.RANKING_RAW_DATA,fd)

def save_update_raw_data(mp,fd):
    fd['updated_at']=datetime.now()
    mp.save(mp.RANKING_RAW_DATA,fd)

class AppInfo(object):
    def save_app(self,aid,elm):
        if not self.mp.is_exists(self.mp.APP_INFO_DATA,{"aid":aid}):
            elm['created_at']=datetime.now()
            elm['aid']=aid
            self.mp.save(self.mp.APP_INFO_DATA,elm)
    def __init__(self,elm,mp):
        att=elm['id']
        aid=att['attributes']['im:id']
        aid=int(aid)
        self.mp=mp
        self.save_app(aid,elm)

def parse_ranking_info(fd,rdict,mp):
    feed=fd['feed']
    link_id=feed['id']['label']
    fd['link_id']=link_id
    if not 'entry' in feed:return 
    for elm in feed['entry']:
        if not 'id' in elm:return
        app=AppInfo(elm,mp)
        if 'summary' in elm:
            del elm['summary']

def get_params(mp):
    for f in mp.find_all(mp.FEED_INFO,{}):
        c=f['country']
        m=f['mediatype']
        for t in f['types']:
            yield (c,m,t)

def move_app_info(mp):
    rdict={}
    date_dict={}
    for i,(country,mediatype,t) in enumerate(get_params(mp)):
        fd=mp.find_sort_all(mp.RANKING_RAW_DATA,dict(updated_at={"$exists":False},country=country,mediatype=mediatype,fieldtype=t['name']),[('created_at',pymongo.DESCENDING)])
        if fd:
            print i,country,mediatype,t
            if fd.count()>0:
                for k in fd:
                    print k['created_at']
                    parse_ranking_info(k,rdict,mp)
                    save_update_raw_data(mp,k)
def main():
    mp=mog_op.MongoOp('localhost')
    move_app_info(mp)

if __name__=='__main__':main()
