#!/bin/bash

HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"

obj_pkg_name="com.particlenews.newsbreak"
#obj_pkg_name="com.machsystem.gawii"
#obj_pkg_name="all.board.result"
#obj_pkg_name="com.manhajona.alMaghamsi"
#obj_pkg_name="com.mobilesrepublic.appy"
TEST_DATA_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/ed_join_click_transform/json"
USER_INS_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/user_interest_vec/json"
APPS_VECTOR_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/apps_vector/json"
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

$sparksubmit script/user_inst_vec_online_evaluate.py $TEST_DATA_PATH $USER_INS_PATH $APPS_VECTOR_PATH $obj_pkg_name  $DES_PATH
