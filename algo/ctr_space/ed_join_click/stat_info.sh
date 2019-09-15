#!/bin/bash

ROOT_PATH=`pwd`

JOB_PATH="$ROOT_PATH/"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/ling.fang/spark_eventlog"

ED_PATH="/data/external/dw/dw_usage_naga_dsp_ed_raw_d/*/*/*"
CLK_PATH="/data/external/dw/dw_usage_naga_dsp_click_raw_d/*/*/*"
DES_PATH="/user/ling.fang/ctr_space/plid_stat_info"

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad \
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

$sparksubmit ${JOB_PATH}/stat_info.py $ED_PATH $CLK_PATH $DES_PATH
