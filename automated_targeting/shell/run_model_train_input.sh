#!/bin/bash

kinit -kt /home/ling.fang/ad_user.keytab ad_user
ROOT_PATH=`pwd`

JOB_PATH="$ROOT_PATH/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/dmp/spark_eventlog"

#obj_pkg_name="com.particlenews.newsbreak"
#obj_pkg_name="com.machsystem.gawii"
obj_pkg_name="com.zymobile.jewel.candy.cookie"
FEA_VEC_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/input/$obj_pkg_name/lookalike_samples/json"
OUT_RESULT_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/input/$obj_pkg_name/lookalike_result/"

${HADOOP_HOME}/bin/hadoop fs -rm -r ${DES_PATH}

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.normal \
                 --num-executors 50 \
                 --executor-memory 8g \
                 --driver-memory 15g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=120 \
                 --conf spark.dynamicAllocation.minExecutors=60 \
                 --conf spark.driver.maxResultSize=15G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/model_train_input.py $FEA_VEC_PATH $OUT_RESULT_PATH $obj_pkg_name
