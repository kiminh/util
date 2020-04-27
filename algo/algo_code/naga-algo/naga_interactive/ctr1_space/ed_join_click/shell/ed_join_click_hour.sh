#!/bin/bash

ROOT_PATH=`pwd`
USER_NAME="ad_user"

JOB_PATH="$ROOT_PATH/script"
HADOOP_HOME="/usr/local/hadoop-ha_new/"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6_new/"
LOG_PATH="hdfs:///user/$USER_NAME/spark_eventlog"
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file

if [ $# -eq 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 hours ago " +%Y%m%d%H`
fi
YESTDAY=`date -d "1 hours ago ${DATE:0:8} ${DATE:8:10}" +"%Y%m%d%H"`

FLAG=flag_set_dw_usage_ad_naga_interactive_h_cn
done_file="${FLAG}##$DATE"
loop_check $done_file 140 2

JOIN_PATH="/user/$USER_NAME/naga_interactive/ctr1_space/ed_join_click_hour/join/$DATE"
YESTDAY_JOIN_PATH="/user/$USER_NAME/naga_interactive/ctr1_space/ed_join_click_hour/join/$YESTDAY"
UNJOIN_PATH="/user/$USER_NAME/naga_interactive/ctr1_space/ed_join_click_hour/unjoin/$DATE"

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

$sparksubmit ${JOB_PATH}/ed_join_click_hour.py $JOIN_PATH $YESTDAY_JOIN_PATH $UNJOIN_PATH $DATE
