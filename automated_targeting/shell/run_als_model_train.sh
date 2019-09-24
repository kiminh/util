#!/bin/bash

HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"

DATA_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/usage_index/json"
#CF_MODEL_PATH="/user/ling.fang/automated_targeting/1/2/3/4/5/model/cf/"
CF_MODEL_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/model/cf/"
#$HADOOP_HOME/bin/hadoop fs -rm -r $CF_MODEL_PATH

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.normal \
                 --num-executors 50 \
                 --executor-memory 12g \
                 --driver-memory 15g \
                 --executor-cores 4 \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.driver.maxResultSize=15G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.yarn.executor.memoryOverhead=10G"

$sparksubmit script/als_model_train_and_test.py $DATA_PATH $CF_MODEL_PATH
