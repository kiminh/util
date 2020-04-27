#!/bin/bash

ROOT_PATH=`pwd`
USER_NAME="ad_user"

JOB_PATH="$ROOT_PATH/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/$USER_NAME/spark_eventlog"

if [ $# -eq 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi
DATE_LIST="{"
for i in `seq 0 6`;do
    day=`date -d " $i days ago $DATE " +%Y%m%d`
    DATE_LIST+=$day,
done
DATE_LIST+="}"

DATA_PATH="/user/$USER_NAME/naga_interactive/ctr1_space/ed_join_click/join/$DATE_LIST"
OUT_PATH="/user/$USER_NAME/naga_interactive/creative_select/icon_ctr/$DATE"
OUT_PATH1="/user/$USER_NAME/naga_interactive/creative_select/feeds_ctr/$DATE"

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

$sparksubmit ${JOB_PATH}/stat_cover_image_ctr.py $DATA_PATH $OUT_PATH $OUT_PATH1 $DATE
