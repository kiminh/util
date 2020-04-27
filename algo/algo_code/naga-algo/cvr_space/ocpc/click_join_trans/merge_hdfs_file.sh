#!/bin/bash

ROOT_PATH=`pwd`
USER_NAME="ad_user"

JOB_PATH="$ROOT_PATH/"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/$USER_NAME/spark_eventlog"

if [ $# -eq 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi
YESTDAY=`date -d "1 days ago $DATE" +"%Y%m%d"`

CLK_PATH="/user/$USER_NAME/ocpc/click_join_trans/clk_path/"
TRANS_PATH="/user/$USER_NAME/ocpc/click_join_trans/trans_path/"
SSPSTAT_PATH="/user/$USER_NAME/ocpc/click_join_trans/sspstat_path/"

#$HADOOP_HOME/bin/hadoop fs -rm -r -f $JOIN_PATH $YESTDAY_JOIN_PATH $UNJOIN_PATH

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 50 \
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

$sparksubmit ${JOB_PATH}/merge_hdfs_file.py $CLK_PATH $TRANS_PATH $SSPSTAT_PATH $DATE
