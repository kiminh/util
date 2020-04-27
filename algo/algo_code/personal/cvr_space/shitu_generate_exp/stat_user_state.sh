#!/bin/bash

ROOT_PATH=`pwd`

JOB_PATH="$ROOT_PATH/"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/ad_user/spark_eventlog"

if [ $# -ge 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi
DATE_LST="{"dt=$DATE
for idx in `seq 1 30`;do
    day=`date -d "$idx days ago $DATE" +"%Y%m%d"`
    DATE_LST=${DATE_LST}",""dt="$day
done
DATE_LST=${DATE_LST}"}"

SRC_PATH="/data/external/ods/ods_usage_data_h/usage_type=usage_naga_dsp_click/${DATE_LST}/*/*"
DES_PATH="/user/ad_user/user_stat/json/"

#${HADOOP_HOME}/bin/hadoop fs -rm -r ${TRAIN_PATH} ${TEST_PATH} ${CVM_PATH}

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 50 \
                 --executor-memory 12g \
                 --driver-memory 12g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=120 \
                 --conf spark.dynamicAllocation.minExecutors=60 \
                 --conf spark.driver.maxResultSize=8G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.sql.broadcastTimeout=36000 \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/stat_user_state.py $SRC_PATH $DES_PATH

