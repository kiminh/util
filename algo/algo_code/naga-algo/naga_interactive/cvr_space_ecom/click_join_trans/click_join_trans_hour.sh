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
[ $# -eq 2 ] && VERSION=$2 || VERSION=`date +%Y%m%d%H%M%S`
[ $# -eq 3 ] && JOB_TAG=$3 || JOB_TAG=""

HOUR=`date -d " 1 hours ago " +%Y%m%d%H`

FLAG_PREFIX=flag_set_dw_usage_naga_dsp
log_type="click transform"
for tp in $log_type;do
    done_file="${FLAG_PREFIX}_${tp}_h_cn##$HOUR"
    loop_check $done_file 140 2
done

JOIN_PATH="/user/$USER_NAME/naga_interactive/ocpc_ecom/click_join_trans${JOB_TAG}/join_path/$VERSION"
PLAN_PATH="/user/$USER_NAME/naga_interactive/ocpc_ecom/click_join_trans${JOB_TAG}/plan_path/$VERSION"
$HADOOP_HOME/bin/hadoop fs -rm -r -f $JOIN_PATH $PLAN_PATH $PKG_PATH

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

$sparksubmit ${JOB_PATH}/click_join_trans_hour.py $JOIN_PATH $PLAN_PATH $DATE
