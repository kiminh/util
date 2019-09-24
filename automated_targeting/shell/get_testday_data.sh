#!/bin/bash

kinit -kt /home/ling.fang/ling.fang.keytab ling.fang

#if [[ $# -lt 1 ]];then
#    echo "get_testday_data.sh days"
#    exit 1
#fi

days="{"
for i in `seq 1 46`;
do
    day=`date -d "$i day 20190515" +%Y%m%d`
    days+=$day","
done
days+="}"

#day=$1

JOB_PATH="`pwd`/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/dmp/spark_eventlog"
ED_PATH="/data/ad/ad_online/$days/*_dsp_ed/*/*"
CLICK_PATH="/data/ad/ad_online/$days/*_dsp_click/*/*"
CONV_PATH="/data/ad/ad_online/$days/*_dsp_transform/*/*"
DES_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/ed_join_click_transform/"

${HADOOP_HOME}/bin/hadoop fs -rm -r ${DES_PATH}

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.normal \
                 --num-executors 50 \
                 --executor-memory 8g \
                 --driver-memory 15g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=120 \
                 --conf spark.dynamicAllocation.minExecutors=60 \
                 --conf spark.driver.maxResultSize=15G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/get_testday_data.py $ED_PATH $CLICK_PATH $CONV_PATH $DES_PATH
