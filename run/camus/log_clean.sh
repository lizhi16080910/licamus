#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath
log_file=$1
#log_file=log.out1
log_size=`ls -al $log_file | awk '{print $5}'`
log_size_max=`expr 50 \* 1024 \* 1024`
echo "log size is $log_size"
echo "log size max is $log_size_max"
if [ "$log_size" -gt "$log_size_max" ]
then
   rm -f $log_file
fi

