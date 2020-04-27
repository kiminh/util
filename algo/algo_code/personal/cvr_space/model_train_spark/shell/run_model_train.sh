#!/bin/bash

bash /home/ling.fang/kinit_ad_user.sh

if [ $# -lt 1 ];then
    echo "Usage:./run_model_train.sh DATE"
    exit 1
fi

DATE=$1

[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
ROOT_DIR=`pwd`
USER_NAME="ad_user"

JOB_PATH="${ROOT_DIR}/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/${USER_NAME}/ocpc/spark_eventlog"
FEA_VEC_PATH="/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/extract_ins_fea/$DATE/ins"
OUT_RESULT_PATH="/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/model_output/$DATE"

common_file="/home/ling.fang/script/tools/common.sh"
source $common_file

#--conf spark.sql.parquet.enableVectorizedReader=true \
#--conf spark.memory.fraction=0.85 \
#--conf spark.shuffle.memory.fraction=0.5 \
#--conf spark.storage.memoryFraction=0.4 \
#--conf spark.sql.parquet.enableVectorizedReader=true \
#--conf spark.memory.fraction=0.75 \
#--conf spark.shuffle.memory.fraction=0.5 \
#--conf spark.executor.cores=4 \
#--conf spark.driver.memoryOverhead=2g \
#--conf spark.executor.memoryOverhead=8g \

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 60 \
                 --executor-memory 15g \
                 --driver-memory 16g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=120 \
                 --conf spark.dynamicAllocation.minExecutors=50 \
                 --conf spark.driver.maxResultSize=15G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.sql.parquet.enableVectorizedReader=true \
                 --conf spark.memory.fraction=0.75 \
                 --conf spark.shuffle.memory.fraction=0.5 \
                 --conf spark.executor.cores=2 \
                 --conf spark.driver.memoryOverhead=2g \
                 --conf spark.executor.memoryOverhead=8g \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.eventLog.dir=$LOG_PATH"

#TEST_DATE=`date -d "0 days ago $DATE" +"%Y%m%d"`
$sparksubmit ${JOB_PATH}/model_train.py $FEA_VEC_PATH $OUT_RESULT_PATH $TEST_DATE
#[ $? -eq 0 ] && \
#clear_hadoop_data $OUT_RESULT_PATH 30 $DATE && \
#{ mylog "model train task success!"; exit 0;} || { mylog "model train task fail!"; exit -1;}

