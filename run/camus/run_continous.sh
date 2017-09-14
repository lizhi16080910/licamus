#!/bin/bash

basepath=$(cd `dirname $0`; pwd)
cd $basepath 
#time=`date +%Y/%m/%d/%H/%M`
dir="/user/cdnlog_duowan/exec/offset/*/offset.info"
while [ "1" -eq "1" ]
do
   echo "a"
   n=`hadoop fs -ls $dir | wc -l`
   if [ "$n" -ne "0" ];then
	echo "start"
        sh submitJob.sh 
	sh domainReport.sh
        sh log_clean.sh log.out
   else
     echo sleep
     sleep 60
   fi
 #  sleep 100
done
