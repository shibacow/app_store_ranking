#!/usr/bin/python
# -*- coding:utf-8 -*-
import requests
import simplejson
import re
from datetime import datetime
import logging
import os
import selector_info
import mog_op
import pprint

logdir=os.path.abspath(os.path.dirname(__file__))
logfile='%s/result.log' % logdir

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=logfile,
                    filemode='a')

def get_ranking(mp):
    #aa=mp.rrdata.group(key={"fieldtype":1,"mediatype":1,"country":1},condition={},initial={"csum":0},\
    #                       reduce="function(obj,prev){prev.csum=prev.csum+1}")
    #for a in aa:
    #    pprint.pprint(a)
    for a in mp.rrdata.find().sort([("created_at",-1)]).limit(40):
        feed=a['feed']
        del a['feed']

        entry=feed['entry']
        del feed['entry']
        print '='*40

        print len(entry)
        print '='*80
        if isinstance(entry,list):
            for e in entry[2:]:
                print '-'*70
                pprint.pprint(a)
                print '*'*50
                pprint.pprint(feed)
                print '='*50
                pprint.pprint(e)
        else:
            print '-'*70
            pprint.pprint(a)
            print '*'*50
            pprint.pprint(feed)
            print '='*50
            pprint.pprint(entry)

def get_app_info(mp):
    for a in mp.appdata.find().sort([("aid",-1)]).limit(40):
        print '*'*30
        pprint.pprint(a)


def main():
    mp=mog_op.MongoOp('localhost')
    #get_ranking(mp)
    get_app_info(mp)

if __name__=='__main__':main()
