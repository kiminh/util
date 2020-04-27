#!/bin/bash

ROOT_DIR=`pwd`
USER_NAME="ad_user"

JOB_PATH="$ROOT_DIR/"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/$USER_NAME/spark_eventlog"

common_file="$ROOT_DIR/../../../../script/tools/common.sh"
source $common_file

if [ $# -ge 1 ];then
    DATE=$1
else
    DATE=`date -d " 0 days ago " +%Y%m%d`
fi

[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
DATE_LIST="{"
for i in `seq 1 7`;do
    day=`date -d " $i days ago $DATE" +%Y%m%d`
    DATE_LIST+=${day},
    done_file="/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/shitu/ins/${day}/_SUCCESS"
    loop_check $done_file 140 1
done
DATE_LIST+="}"
INPUTDIR="/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/shitu/ins/${DATE_LIST}"
OUTPUTDIR="/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/shitu_shuffle/${DATE}/"
$HADOOP_HOME/bin/hadoop fs -rm -r -f $OUTPUTDIR

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 50 \
                 --executor-memory 12g \
                 --driver-memory 12g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=120 \
                 --conf spark.dynamicAllocation.minExecutors=60 \
                 --conf spark.driver.maxResultSize=8G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.sql.broadcastTimeout=36000 \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/shuffle.py $INPUTDIR $OUTPUTDIR $DATE
