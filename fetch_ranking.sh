#!/bin/bash
BASEDIR=$(dirname $0)
cd $BASEDIR
echo $BASEDIR
./get_feed_info.py | tee $BASEDIR/get_feed_info.log
./get_ranking_info.py|tee $BASEDIR/get_ranking_info.log
./check_app_info.py |tee $BASEDIR/check_app_info.log
