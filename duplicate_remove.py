#!/usr/bin/python
# -*- coding:utf-8 -*-
import re
from datetime import datetime,timedelta
import logging
import os
import mog_op

logdir=os.path.abspath(os.path.dirname(__file__))
logfile='%s/result.log' % logdir

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=logfile,
                    filemode='a')

def remove_duplicate_object(mp,r,icnt):
    ddict={}
    date=r['dates']
    for d in date:
        dy=d.strftime("%Y-%m-%d")
        ddict.setdefault(dy,0)
        ddict[dy]+=1
    delcnt=0
    for a,b in sorted(ddict.items()):
        prev=datetime.strptime(a,"%Y-%m-%d")
        to=prev+timedelta(days=1)
        cond=dict(country=r['country'],mediatype=r['mediatype'],fieldtype=r['fieldtype'])
        cond['fetch_date']={"$gte":prev,"$lt":to}
        rr=mp.find_all(mp.RANKING_META_DATA,cond)
        cnt=rr.count()
        rra=[r for r in rr]
        rra=sorted(rra,key=lambda x:x['fetch_date'])
        msg="i=%d" % icnt
        logging.info(msg)
        for i,kk in enumerate(rra):
            if i>0:
                rid=kk['ranking_raw_id']
                mp.remove_id(mp.RANKING_RAW_DATA,rid)
                mid=kk['_id']
                mp.remove_id(mp.RANKING_META_DATA,mid)
                delcnt+=1
                msg='icnt=%d delcnt=%d country=%s mediatype=%s fieldtype=%s created_at=%s' % \
                (icnt,delcnt,r['country'],r['mediatype'],r['fieldtype'],prev)
                print msg
                logging.info(msg)

def group_meta(mp):
    fd=datetime(2013,1,1)
    to=datetime.now()
    dk={'key':{'country':True,'mediatype':True,'fieldtype':True},
        'condition':{'fetch_date':{'$lt':to,'$gte':fd}},
        'reduce':'function(prev,obj){obj.dates.push(prev.fetch_date);}',
        'initial':{'dates':[]}}
    rr=mp.group(mp.RANKING_META_DATA,dk)
    for i,r in enumerate(rr):
        remove_duplicate_object(mp,r,i)

def main():
    mp=mog_op.MongoOp('localhost')
    group_meta(mp)
if __name__=='__main__':main()
