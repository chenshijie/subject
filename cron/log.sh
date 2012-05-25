#!/bin/sh
mv /home/jason/workspace/StockRadar/subject/log/subject.log /home/jason/workspace/StockRadar/subject/log/subject.`date +%Y%m%d -d -1day`.log
cd /home/jason/workspace/StockRadar/subject 
kill -USR2 `cat run/server.lock`
