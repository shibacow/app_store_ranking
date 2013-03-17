#!/bin/bash
BASEDIR=$(dirname $0)
cd $BASEDIR
echo $BASEDIR
./get_feed_info.py
./get_ranking_info.py
./check_app_info.py
