#!/bin/bash

HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"

DATA_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/usage_tfidf/json"
SI_MODEL_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/model/stringindex/"
DES_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/usage_index/"

#$HADOOP_HOME/bin/hadoop fs -rm -r -f $SI_MODEL_PATH
$HADOOP_HOME/bin/hadoop fs -rm -r -f $DES_PATH

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master local[4] \
                 --queue root.ad-root.etl.dailyetl.normal \
                 --num-executors 50 \
                 --executor-memory 8g \
                 --driver-memory 8g \
                 --executor-cores 4 \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.driver.maxResultSize=10G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.yarn.executor.memoryOverhead=10G"

$sparksubmit script/vectorizer.py $DATA_PATH $SI_MODEL_PATH $DES_PATH
