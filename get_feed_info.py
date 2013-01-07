#!/usr/bin/python
# -*- coding:utf-8 -*-
import requests
import simplejson
#from xml.dom import minidom
import re
from datetime import datetime
import logging
import os
import time
import selector_info
import mog_op

logdir=os.path.abspath(os.path.dirname(__file__))
logfile='%s/result.log' % logdir

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=logfile,
                    filemode='a')

feedinfos='http://itunes.apple.com/WebObjects/MZStoreServices.woa/wa/RSS/wsAvailableFeeds?cc=%s'

def getCountry():
    return [k for k in selector_info.country_list]
    #return ['JP','US','TW','CH']
def getMediaType():
    pass
def save_raw_data(c,fd,mp):
    fdata=mp.is_exists(mp.FEED_RAW_DATA,{'country':c})
    if fdata:
        fdata['raw_data']=fd
        mp.save(mp.FEED_RAW_DATA,fdata)
    else:
        mp.save(mp.FEED_RAW_DATA,dict(country=c,raw_data=fd))

class FeedInfo(object):
    def __genres(self,fdlist):
        genres=fdlist['genres']
        self.genreids=[]
        for gl in genres['list']:
            self.genreids.append(dict(id=gl['value'],name=gl['display']))
    def __types(self,fdlist):
        types=fdlist['types']
        #print types['display']
        self.types=[]
        for tl in types['list']:
            #print tl['display'],tl['name'],tl['urlPrefix']
            self.types.append(dict(name=tl['name'],urlPrefix=tl['urlPrefix'],display=tl['display']))
    def to_json(self):
        return dict(country=self.country,mediatype=self.mediatype,genres=self.genreids,\
                        types=self.types,mediadisplay=self.mediadisplay,created_at=self.created_at)
    def __init__(self,c,fdlist):
        self.country=c
        self.created_at=datetime.now()
        self.mediatype=fdlist['name']
        self.mediadisplay=fdlist['display']
        self.__genres(fdlist)
        self.__types(fdlist)

def getFeedInfo(r,c,mp):
    print c
    tt=re.sub('^availableFeeds=','',r.text)
    fd=simplejson.loads(tt)
    save_raw_data(c,fd,mp)
    for f in fd['list']:
        fi=FeedInfo(c,f)
        finfo=mp.is_exists(mp.FEED_INFO,{"country":fi.country,"mediatype":fi.mediatype})
        if finfo:
            kv=fi.to_json()
            for k in kv:
                finfo[k]=kv[k]
            mp.save(mp.FEED_INFO,finfo)
        else:
            mp.save(mp.FEED_INFO,fi.to_json())
            
            

def main():
    mp=mog_op.MongoOp('localhost')
    for c in getCountry():
        url=feedinfos % c
        r=requests.get(url)
        getFeedInfo(r,c,mp)
        time.sleep(0.5)

if __name__=='__main__':main()
