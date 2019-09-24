#!/bin/bash

kinit -kt /home/ling.fang/ad_user.keytab ad_user

end_time=`date -d '-2 day' +%Y%m%d`
echo $start_time
start_time=`date -d '-9 day' +%Y%m%d`
echo $end_time
day_timestamp=`date -d '0 day' +%Y%m%d`

start_time="20190707"
end_time="20190724"

ROOT_PATH=`pwd`
JOB_PATH="$ROOT_PATH/script"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
LOG_PATH="hdfs:///user/dmp/spark_eventlog"
APPDB_PATH="/data/dw/app_db/latest/json/*"
DES_PATH="/user/ling.fang/1/2/3/4/5/automated_targeting/input/appinstall/"

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

$sparksubmit ${JOB_PATH}/extract_appinstall_input.py $APPDB_PATH $DES_PATH $start_time $end_time
