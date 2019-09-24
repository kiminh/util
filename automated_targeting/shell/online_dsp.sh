#!/bin/bash

kinit -kt /home/ling.fang/ling.fang.keytab ling.fang

ROOT_PATH=`pwd`
JOB_PATH="$ROOT_PATH/script/"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/dmp/spark_eventlog"

days="{"
for i in `seq 1 8`;
do
    day=`date -d "-$i day" +%Y%m%d`
    days+=$day","
done
days+="}"

REQ_PATH="/data/ad/ad_online/$days/*_dsp_request/{us,ap,eu}/*"
ED_PATH="/data/ad/ad_online/$days/*_dsp_ed/{us,ap,eu}/*"

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.normal \
                 --num-executors 50 \
                 --executor-memory 8g \
                 --driver-memory 10g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=120 \
                 --conf spark.dynamicAllocation.minExecutors=50 \
                 --conf spark.driver.maxResultSize=12G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/online_dsp.py $REQ_PATH $ED_PATH
