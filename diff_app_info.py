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
def parse_ranking_info(fd,rdict):
    feed=fd['feed']
    link_id=feed['id']['label']
    fd['link_id']=link_id
    #save_raw_data(mp,fd)
    if not 'entry' in feed:return 
    for elm in feed['entry']:
        #print '='*60
        if not 'id' in elm:return
        att=elm['id']
        #print att
        aid=att['attributes']['im:id']
        aid=int(aid)
        if not 'summary' in elm:
            rdict.setdefault(aid,0)
            rdict[aid]+=1
        
        #for k in elm:
        #    if k=='summary':continue
        #    print k,type(elm[k]),elm[k]
        #summary=elm['summary']['label']
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
        
def parse_date(fd,date_dict):
    dt=fd['created_at']
    dt=dt.strftime('%Y-%m-%d_%H')
    date_dict.setdefault(dt,0)
    date_dict[dt]+=1

def count_sz(mp):
    rdict={}
    date_dict={}
    for i,(country,mediatype,t) in enumerate(get_params(mp)):
        fd=mp.find_all(mp.RANKING_RAW_DATA,dict(country=country,mediatype=mediatype,fieldtype=t['name']))
        if fd:
            print i,country,mediatype,t
            if fd.count()>0:
                for k in fd:
                    parse_date(k,date_dict)
                    parse_ranking_info(k,rdict)
    for (a,b) in sorted(rdict.items(),key=lambda x:x[1],reverse=True)[:100]:
        print a,b
    sz=len(rdict)
    sz2=sum([t[1] for t in rdict.items()])
    print sz,sz2,(sz2*1.0)/sz
    for (a,b) in sorted(date_dict.items(),key=lambda x:x[0]):
        print a,b

def main():
    mp=mog_op.MongoOp('localhost')
    count_sz(mp)

if __name__=='__main__':main()
