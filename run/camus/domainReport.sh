#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath

yarn jar camus-1.0-SNAPSHOT.jar com.fastweb.cdnlog.bigdata.duowan.together.DomainReportMix camus.properties
