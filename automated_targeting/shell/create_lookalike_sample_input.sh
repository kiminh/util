#!/bin/bash

kinit -kt /home/ling.fang/ad_user.keytab ad_user

ROOT_PATH=`pwd`
JOB_PATH="$ROOT_PATH/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/dmp/spark_eventlog"

country="US"
#obj_pkg_name="com.particlenews.newsbreak"
#obj_pkg_name="com.machsystem.gawii"
obj_pkg_name="com.zymobile.jewel.candy.cookie"

#SRC_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/matrix/user_applist_all/json"
#SRC_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/input/user_interest_vec/json"
SRC_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/input/$country/usage_tfidf/json"
APPS_VECT_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/apps_vector_tmp/json"
SEED_USER_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/input/$country/$obj_pkg_name/seed_user/json"
DES_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/input/$country/$obj_pkg_name/lookalike_samples/"

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.normal \
                 --num-executors 50 \
                 --executor-memory 12g \
                 --driver-memory 15g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=120 \
                 --conf spark.dynamicAllocation.minExecutors=50 \
                 --conf spark.driver.maxResultSize=12G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/create_lookalike_sample_input.py $SRC_PATH $APPS_VECT_PATH $SEED_USER_PATH $DES_PATH
