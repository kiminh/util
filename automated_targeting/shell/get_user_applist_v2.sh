#!/bin/bash

kinit -kt /home/ling.fang/ling.fang.keytab ling.fang

ROOT_PATH=`pwd`

JOB_PATH="$ROOT_PATH/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/dmp/spark_eventlog"
APPLIST_PATH="/data/dw/applist/{us,ap,eu}/parquet"
APPDB_PATH="/data/dw/app_db/parquet/latest/"
UID2GAID="/user/james.jiang/1/2/3/4/5/all_ids/matrix/{ap,us,eu}/latest/"
DES_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/matrix/user_applist_all/"

${HADOOP_HOME}/bin/hadoop fs -rm -r ${DES_PATH}

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.normal \
                 --num-executors 60 \
                 --executor-memory 10g \
                 --driver-memory 15g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=100 \
                 --conf spark.dynamicAllocation.minExecutors=60 \
                 --conf spark.driver.maxResultSize=12G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.executor.cores=2 \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/get_user_applist_v2.py $APPLIST_PATH $UID2GAID $DES_PATH
