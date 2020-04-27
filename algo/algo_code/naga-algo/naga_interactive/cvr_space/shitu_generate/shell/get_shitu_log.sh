#!/bin/bash

ROOT_PATH=`pwd`
USER_NAME="ad_user"

JOB_PATH="$ROOT_PATH/script"
#HADOOP_HOME="/usr/local/hadoop-ha_new/"
#SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6_new/"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/$USER_NAME/spark_eventlog"
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file

if [ $# -ge 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi
DATE_YESTDAY=`date -d "1 days ago $DATE" +"%Y%m%d"`
DATE_LST="{"$DATE
for idx in `seq 1 25`;do
    day=`date -d "$idx days ago $DATE" +"%Y%m%d"`
    DATE_LST=${DATE_LST}","$day
done
DATE_LST=${DATE_LST}"}"
done_path="/user/${USER_NAME}/ocpc/click_join_trans/temp/${DATE}/_SUCCESS"
loop_check $done_path 140 1

[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
ROOT_DIR=`pwd`

JOIN_PATH="/user/${USER_NAME}/ocpc/click_join_trans/{temp/${DATE},stable/${DATE_LST}}"
SHITU_LOG_PATH="/user/$USER_NAME/naga_interactive/ocpc/shitu_log${JOB_TAG}/${DATE}/"

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 5 \
                 --executor-memory 8g \
                 --driver-memory 12g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=100 \
                 --conf spark.dynamicAllocation.minExecutors=50 \
                 --conf spark.driver.maxResultSize=8G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.sql.broadcastTimeout=36000 \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/get_shitu_log.py $JOIN_PATH $SHITU_LOG_PATH $DATE
