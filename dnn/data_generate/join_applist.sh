#!/bin/bash

ROOT_PATH=`pwd`
USER_NAME="ad_user"

JOB_PATH="$ROOT_PATH/"
#HADOOP_HOME="/usr/local/hadoop-2.6.3"
HADOOP_HOME="/usr/local/hadoop-ha_new/"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
#SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6_new/"
LOG_PATH="hdfs:///user/$USER_NAME/spark_eventlog"

ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file

if [ $# -ge 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi

DATE_LST="{"$DATE
for idx in `seq 1 30`;do
    day=`date -d "$idx days ago $DATE" +"%Y%m%d"`
    DATE_LST=${DATE_LST}","$day
done
DATE_LST=${DATE_LST}"}"

DATA_PATH="/user/ad_user/naga_interactive/ocpc/shitu_log/20200412/"
#DATA_PATH="/user/ad_user/naga_interactive/ocpc/click_join_trans/join_path/20200413230940"
APPLIST_PATH="/user/ad_user/private_user/james.jiang/applist/{old,new}/final/$DATE_LST/*"
DES_PATH="/user/ad_user/naga_interactive/ocpc/shitu_log_with_applist/20200412/"
#DES_PATH="/user/ad_user/naga_interactive/ocpc/shitu_log_with_applist/20200413/"

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 5 \
                 --executor-memory 4g \
                 --driver-memory 10g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=40 \
                 --conf spark.dynamicAllocation.minExecutors=5 \
                 --conf spark.driver.maxResultSize=8G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.sql.broadcastTimeout=36000 \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/join_applist.py $DATA_PATH $APPLIST_PATH $DES_PATH
