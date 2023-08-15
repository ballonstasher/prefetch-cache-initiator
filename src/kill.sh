#!/bin/bash 

pid=`ps -ef | grep main.py | grep -v grep | awk '{print $2}'`

sudo kill -9 $pid
