#!/bin/bash

basepath=$(cd `dirname $0`; pwd)
cd $basepath
nohup sh run_continous.sh >>log.out 2>&1 &
