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
import copy
import pprint
import grequests

logdir=os.path.abspath(os.path.dirname(__file__))
logfile='%s/result.log' % logdir

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=logfile,
                    filemode='a')

ranking_info='https://itunes.apple.com/jp/rss/topfreeapplications/limit=300/json'
ranking_info='https://itunes.apple.com/jp/rss/topfreeapplications/limit=10/json'

class SaveFeed(object):
    def __init__(self,mp,fd):
        self.mp=mp
        self.fd=fd
        self.parse_ranking_info(mp,fd)
    def save_app(self,aid,elm):
        self.mp.remove(self.mp.APP_INFO_DATA,aid)
        elm['created_at']=datetime.now()
        elm['aid']=aid
        self.mp.save(self.mp.APP_INFO_DATA,elm)
    def save_raw_data(self,mp,fd):
        fd['created_at']=datetime.now()
        mp.save(mp.RANKING_RAW_DATA,fd)
    def get_aid(self,elm):
        att=elm['id']
        aid=att['attributes']['im:id']
        aid=int(aid)
        #msg="upsert aid=%d" % aid
        #print msg
        #logging.info(msg)
        self.save_app(aid,elm)
    def parse_ranking_info(self,mp,fd):
        feed=fd['feed']
        link_id=feed['id']['label']
        fd['link_id']=link_id
        if not 'entry' in feed:return
        if isinstance(feed['entry'],dict):
            elm=feed['entry']
            e2=copy.deepcopy(elm)
            self.get_aid(e2)
            if 'summary' in elm:
                del elm['summary']
        elif isinstance(feed['entry'],list):
            for elm in feed['entry']:
                e2=copy.deepcopy(elm)
                self.get_aid(e2)
                if 'summary' in elm:
                    del elm['summary']
        self.save_raw_data(mp,fd)

def get_info(r):
    kk=None
    try:
        kk=simplejson.loads(r.content)
    except simplejson.decoder.JSONDecodeError,err:
        print err
        logging.error(err)
        return None
    except requests.exceptions.ConnectionError,err:
        print err
        logging.error(err)
        return None
    return kk
def get_params(mp):
    for f in mp.find_all(mp.FEED_INFO,{}):
        c=f['country']
        m=f['mediatype']
        for t in f['types']:
            yield (c,m,t)
        
def is_already_exists_prev_data(mp,country,mediatype,t):
    c=datetime.now()
    prev=c-timedelta(hours=20)
    cond={"country":country,"mediatype":mediatype,"fieldtype":t['name'],"created_at":{"$gte":prev,"$lt":c}}
    #print cond
    r=mp.find_all(mp.RANKING_RAW_DATA,cond)
    if r.count()>0:
        return True
    else:
        return False
class URL_DICT(object):
    def __init__(self,url,country,mediatype,fieldtype,fieldinfo,msg):
        self.url=url
        self.country=country
        self.mediatype=mediatype
        self.fieldtype=fieldtype
        self.fieldinfo=fieldinfo
        self.msg=msg
    

def sweap(mp,urldict,urllist):
    rs=(grequests.get(u) for u in urllist)
    for r in grequests.imap(rs):
        fd=get_info(r)
        if not fd:continue
        finfo=urldict.get(r.url,None)
        if not finfo:continue
        print finfo.msg
        logging.info(finfo.msg)
        fd['country']=finfo.country
        fd['mediatype']=finfo.mediatype
        fd['fieldtype']=finfo.fieldtype
        fd['fieldinfo']=finfo.fieldinfo
        fd['is_meta_stored']=False
        sv=SaveFeed(mp,fd)
    msg='end sweap dictsize=%d listsize=%d' % (len(urldict),len(urllist))
    print msg
    logging.info(msg)

def main():
    mp=mog_op.MongoOp('localhost')
    count=0
    cattypes=[]
    for i,(country,mediatype,t) in enumerate(get_params(mp)):
        cattypes.append((country,mediatype,t))
    urldict={}
    urllist=[]
    for i,(country,mediatype,t) in enumerate(cattypes):
        u=t['urlPrefix']
        if not re.search('WebObject',u):
            url=u+'limit=300/json'
            count+=1
            msg='count=%d country=%s mediatype=%s fieldtype=%s url=%s' % (count,country,mediatype,t['name'],url)
            print msg
            if is_already_exists_prev_data(mp,country,mediatype,t):
                continue
            urldict[url]=URL_DICT(url,country,mediatype,t['name'],t,msg)
            urllist.append(url)
            if len(urllist)>50:
                sweap(mp,urldict,urllist)
                urllist=[]
    sweap(mp,urldict,urllist)
    urllist=[]
            
if __name__=='__main__':main()
