#!/bin/bash

ROOT_PATH=`pwd`
USER_NAME="ad_user"

JOB_PATH="$ROOT_PATH/script/"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/$USER_NAME/spark_eventlog"

common_file="$ROOT_PATH/../../script/tools/common.sh"
source $common_file

TIME=`date -d " 1 hours ago $TIME " +"%Y%m%d%H"`
DATE=${TIME:0:8}

data_path="/user/ad_user/ctr_space/ed_join_click_hour/join/${DATE}*/"
done_file="/user/ad_user/ctr_space/ed_join_click_hour/join/${TIME}"
out_path="/user/ad_user/ctr_space/calibrate/${TIME}"
loop_check $done_file 360 1

#$HADOOP_HOME/bin/hadoop fs -rm -r -f $JOIN_PATH $YESTDAY_JOIN_PATH $UNJOIN_PATH

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 5 \
                 --executor-memory 8g \
                 --executor-cores 8 \
                 --driver-memory 12g \
                 --conf spark.sql.shuffle.partitions=40 \
                 --conf spark.dynamicAllocation.enabled=false \
                 --conf spark.driver.maxResultSize=8G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.sql.broadcastTimeout=36000 \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/stat_adstyle_edclk.py $data_path $out_path ${TIME}
