#!/usr/bin/python
# -*- coding:utf-8 -*-
import mog_op
from datetime import datetime
from bson.objectid import ObjectId

def parse_date(a):
    return a['fetch_date']
def raw_data(mp,a):
    return mp.rrdata.find({'_id':a['ranking_raw_id']})

def delete_a(mp):
    dkt={}
    cntt=0
    for i,a in enumerate(mp.find_all(mp.RANKING_META_DATA,{})):
        dt=parse_date(a)
        wd=dt.weekday()
        dkt.setdefault(wd,0)
        dkt[wd]+=1
        if dt.weekday()!=6:
            mp.remove_id(mp.RANKING_RAW_DATA,a['ranking_raw_id'])
            mp.remove_id(mp.RANKING_META_DATA,a['_id'])
        if i%100==0:
            print i,dt,wd
    print dkt
def delete_b(mp):
    dkt={}
    print mp
    where={"$where":"function(){return this.fetch_date.getDay()==6;}"}
    #where={"$where":"this.fetch_date.getDay() == 6"}
    
    print where
    for i,a in enumerate(mp.find_all(mp.RANKING_META_DATA,where,1000)):
        dt=parse_date(a)
        wd=dt.weekday()
        dkt.setdefault(wd,0)
        dkt[wd]+=1
        print i,wd,dt

                            
def main():
    mp=mog_op.MongoOp('localhost')
    delete_a(mp)
if __name__=='__main__':main()
