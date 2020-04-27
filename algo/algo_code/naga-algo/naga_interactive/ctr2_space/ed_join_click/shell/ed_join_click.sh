#!/bin/bash

ROOT_PATH=`pwd`
USER_NAME="ad_user"

JOB_PATH="$ROOT_PATH/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/$USER_NAME/spark_eventlog"
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file

if [ $# -eq 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi
YESTDAY=`date -d "1 days ago $DATE" +"%Y%m%d"`
FLAG_PREFIX=flag_set_dw_usage_naga_dsp
log_type="ed click"
for idx in `seq 0 1`;do
    day=`date -d "$idx days ago $DATE" +%Y%m%d`
    for tp in $log_type;do
        done_file="${FLAG_PREFIX}_${tp}_h_cn##$day"
        loop_check $done_file 140 2
    done
done

JOIN_PATH="/user/$USER_NAME/naga_interactive/ctr2_space/ed_join_click/join/$DATE"
YESTDAY_JOIN_PATH="/user/$USER_NAME/naga_interactive/ctr2_space/ed_join_click/join/$YESTDAY"

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 5 \
                 --executor-memory 4g \
                 --driver-memory 12g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=30 \
                 --conf spark.dynamicAllocation.minExecutors=10 \
                 --conf spark.driver.maxResultSize=8G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.sql.broadcastTimeout=36000 \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/ed_join_click.py $JOIN_PATH $YESTDAY_JOIN_PATH $DATE
