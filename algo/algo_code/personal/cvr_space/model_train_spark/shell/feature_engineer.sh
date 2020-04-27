#!/bin/bash

bash /home/ling.fang/kinit_ad_user.sh

if [ $# -eq 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi
#DATE_LST="{"$DATE
#for idx in `seq 1 29`;do
#    day=`date -d "$idx days ago $DATE" +"%Y%m%d"`
#    DATE_LST=${DATE_LST}","$day
#done
#DATE_LST=${DATE_LST}"}"

[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
ROOT_DIR=`pwd`
USER_NAME="ad_user"
JOB_PATH="$ROOT_DIR/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/${USER_NAME}/ocpc/spark_eventlog"
SHITU_PATH="/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/shitu/ins//$DATE/"
FEA_OUTPUT_PATH="/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/extract_ins_fea/$DATE"

common_file="/home/ling.fang/script/tools/common.sh"
source $common_file
loop_check /user/${USER_NAME}/ocpc/model_train${JOB_TAG}/shitu/ins/$DATE/_SUCCESS 144 1

${HADOOP_HOME}/bin/hadoop fs -rm -r ${FEA_OUTPUT_PATH}

#--conf spark.sql.codegen.wholeStage=false \
#--conf spark.executor.cores=1 \
#--conf spark.shuffle.memory.fraction=0.3 \
#--conf spark.shuffle.service.enabled=true \
#--conf spark.memory.fraction=0.85 \
#--conf spark.shuffle.memory.fraction=0.5 \

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 50 \
                 --executor-memory 8g \
                 --driver-memory 15g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=100 \
                 --conf spark.dynamicAllocation.minExecutors=50 \
                 --conf spark.driver.maxResultSize=8G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.executor.cores=1 \
                 --conf spark.driver.memoryOverhead=2g \
                 --conf spark.executor.memoryOverhead=20g \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.eventLog.dir=$LOG_PATH"

#LOC_GP_PATH=${ROOT_DIR}/online_data/data/$DATE
#[ ! -d $LOC_GP_PATH ] && mkdir -p $LOC_GP_PATH
$sparksubmit ${JOB_PATH}/feature_engineer.py $SHITU_PATH  $FEA_OUTPUT_PATH
#[ $? -eq 0 ] && \
#clear_hadoop_data $FEA_OUTPUT_PATH 14 $DATE && \
#{ mylog "feature engineer task success!"; exit 0;} || { mylog "features engineer task fail!"; exit -1;}
