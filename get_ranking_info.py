#!/usr/bin/python
# -*- coding:utf-8 -*-
import requests
import simplejson
import re
from datetime import datetime,timedelta
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
def parse_ranking_info(fd):
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

def get_info(url):
    r=requests.get(url)
    return simplejson.loads(r.text)

def get_params(mp):
    for f in mp.find_all(mp.FEED_INFO,{}):
        c=f['country']
        m=f['mediatype']
        for t in f['types']:
            yield (c,m,t)
        
def is_already_exists_prev_data(mp,country,mediatype,t):
    c=datetime.now()
    prev=c-timedelta(hours=4)
    cond={"country":country,"mediatype":mediatype,"fieldtype":t['name'],"created_at":{"$gte":prev,"$lt":c}}
    #print cond
    r=mp.find_all(mp.RANKING_RAW_DATA,cond)
    if r.count()>0:
        return True
    else:
        return False
    
def main():
    mp=mog_op.MongoOp('localhost')
    count=0
    for i,(country,mediatype,t) in enumerate(get_params(mp)):
        u=t['urlPrefix']
        if not re.search('WebObject',u):
            url=u+'limit=300/json'
            count+=1
            msg='count=%d country=%s mediatype=%s fieldtype=%s url=%s' % (count,country,mediatype,t['name'],url)
            print msg
            if is_already_exists_prev_data(mp,country,mediatype,t):
                continue
            logging.info(msg)
            fd=get_info(url)    
            fd['country']=country
            fd['mediatype']=mediatype
            fd['fieldtype']=t['name']
            fd['fieldinfo']=t
            save_raw_data(mp,fd)
    
if __name__=='__main__':main()
