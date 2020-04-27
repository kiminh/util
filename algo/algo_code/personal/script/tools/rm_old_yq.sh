#!/bin/sh
hadoop=/home/zhulihui/tools/hadoop-client/hadoop-yq/bin/hadoop
[ $# -eq 0 ] && echo "please input hadoop path" && exit 1
hadoop_path=$1
start_date=$2
for i in `seq ${start_date} 30`;do
    date=`date -d "$i days ago" +"%Y%m%d"`
    hadoop fs -test -d $hadoop_path/$date && \
    hadoop fs -rmr $hadoop_path/$date     
done

