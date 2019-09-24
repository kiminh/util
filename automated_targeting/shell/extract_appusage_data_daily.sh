#!/bin/bash

kinit -kt /home/ling.fang/ad_user.keytab ad_user
if [[ $# -ne 1 ]];then
    echo "Usage: bash cmd day"
    exit 1
fi
region="{us,ap,eu}"
day=$1

ROOT_PATH=`pwd`
JOB_PATH="$ROOT_PATH/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/dmp/spark_eventlog"
USER_APPLIST_DATA="/user/ling.fang/age_model/user_applist_data/parquet"
APPUSAGE_PATH="/user/dw/trends/etl/app_usage/$region/$day/"
APPDB_PATH="/data/dw/app_db/latest/json"
IDF2GAID="/user/james.jiang/1/2/3/4/5/all_ids/smartinput/{us,ap,eu}/latest"
DES_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/usage_daily/$day/"

${HADOOP_HOME}/bin/hadoop fs -rm -r ${DES_PATH}

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

$sparksubmit ${JOB_PATH}/extract_appusage_data_daily.py $USER_APPLIST_DATA $APPUSAGE_PATH $APPDB_PATH $IDF2GAID $DES_PATH
