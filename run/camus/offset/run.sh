#!/bin/bash

basepath=$(cd `dirname $0`; pwd)
cd $basepath 
#time=`date +%Y/%m/%d/%H/%M`
time=`date -d '-5 minutes' +%s`
time2=`date -d '-5 minutes' +%Y/%m/%d/%H/%M`
time3=`date -d '-5 minutes' +%Y-%m-%d-%H-%M`
#lastTime=`date +%s -d "2016/10/24 13:25"`
lastTime=`date -d '-10 minutes' +%s`
path=/user/camus-test/offset
localPath=./record
hdfsPath=/user/cdnlog_duowan/exec/offset
#topics=a_topic,cdnlog_c01_i02
topics=cdnlog_parent_new,cdnlog_duowan  #,cdnlog_c06_i06,cdnlog_c01_i02
#brokerList=slave224:9092,slave234:9092,slave226:9092,slave227:9092,slave228:9092,slave229:9092,slave230:9092,slave231:9092,slave232:9092,slave233:9092,slave235:9092,slave236:9092
brokerList=192.168.100.60:9092,192.168.100.61:9092,192.168.100.62:9092,192.168.100.63:9092,192.168.100.64:9092,192.168.100.65:9092,192.168.100.66:9092,192.168.100.67:9092,192.168.100.68:9092,192.168.100.69:9092
yarn jar camus-1.0-SNAPSHOT.jar com.fastweb.cdnlog.bigdata.mapred.custom.OffsetRecorder $time $lastTime $path $topics $brokerList $localPath
hadoop fs -mkdir -p $hdfsPath/$time3
hadoop fs -put $localPath/$time2/offset.info $hdfsPath/$time3/offset.info

