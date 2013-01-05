#!/usr/bin/python
# -*- coding:utf-8 -*-
import requests
import simplejson
#from xml.dom import minidom
import re
from datetime import datetime
import logging
import os
#import time
import selector_info
import mog_op

logdir=os.path.abspath(os.path.dirname(__file__))
logfile='%s/result.log' % logdir

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=logfile,
                    filemode='a')

ranking_info='https://itunes.apple.com/jp/rss/topfreeapplications/limit=300/json'
ranking_info='https://itunes.apple.com/jp/rss/topfreeapplications/limit=10/json'

def save_raw_data(mp,fd):
    fd['created_at']=datetime.now()
    mp.save(mp.RANKING_RAW_DATA,fd)

def main():
    mp=mog_op.MongoOp('localhost')
    r=requests.get(ranking_info)
    fd=simplejson.loads(r.text)

    feed=fd['feed']
    link_id=feed['id']['label']
    fd['link_id']=link_id
    #save_raw_data(mp,fd)
    for elm in feed['entry']:
        print '='*60
        for k in elm:
            if k=='summary':continue
            print k,type(elm[k]),elm[k]
        summary=elm['summary']['label']
    #for k in feed:
    #    if k=='entry':continue
    #    print k,type(feed[k]),feed[k]
    
if __name__=='__main__':main()
