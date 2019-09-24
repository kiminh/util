#!/bin/bash

HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"

#obj_pkg_name="com.particlenews.newsbreak"
#obj_pkg_name="com.machsystem.gawii"
#obj_pkg_name="all.board.result"
#obj_pkg_name="com.manhajona.alMaghamsi"
obj_pkg_name="com.mobilesrepublic.appy"
CF_MODEL_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/model/cf/"
TEST_DATA_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/ed_join_click_transform/json"
SI_MODEL_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/model/stringindex/"
USAGE_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/usage_index/json"
DES_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/online_result/"

$HADOOP_HOME/bin/hadoop fs -rm -r $DES_PATH

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

$sparksubmit script/model_predict_online_evaluate.py $CF_MODEL_PATH $TEST_DATA_PATH $SI_MODEL_PATH $obj_pkg_name $USAGE_PATH $DES_PATH
