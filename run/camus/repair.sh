#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath
date=`date -d "2016/11/20 00:25" +%s`
date1=`date -d @$date +%Y/%m/%d/%H/%M`
date2=`date -d @$date +%Y-%m-%d-%H-%M`
localPath=offset/record
hdfsPath=/user/cdnlog_duowan/exec/offset
hadoop fs -mkdir -p $hdfsPath/$date2
hadoop fs -put $localPath/$date1/offset.info $hdfsPath/$date2/offset.info
