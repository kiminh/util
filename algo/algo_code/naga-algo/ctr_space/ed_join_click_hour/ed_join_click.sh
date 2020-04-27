#!/bin/bash

ROOT_PATH=`pwd`
USER_NAME="ad_user"

JOB_PATH="$ROOT_PATH/"
HADOOP_HOME="/usr/local/hadoop-ha_new/"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6_new"
LOG_PATH="hdfs:///user/$USER_NAME/spark_eventlog"

common_file="$ROOT_PATH/../../script/tools/common.sh"
source $common_file

if [ $# -eq 2 ];then
    DATE=$1
    HOUR=$2
    TIME="$DATE $HOUR"
else
    TIME=`date -d " 1 hours ago " "+%Y%m%d %H"`
    DATE=${TIME:0:8}
    HOUR=${TIME:9:2}
fi

PRETIME=`date -d " 1 hours ago $TIME " +"%Y%m%d%H"`
PREHOUR=${PRETIME:8:2}
PREDATE=${PRETIME:0:8}

ED_PATH="/data/external/ods/ods_usage_data_h/usage_type=usage_naga_dsp_ed/{dt=$DATE/hour=$HOUR,dt=$PREDATE/hour=$PREHOUR}/*"
CLK_PATH="/data/external/ods/ods_usage_data_h/usage_type=usage_naga_dsp_click/{dt=$DATE/hour=$HOUR,dt=$PREDATE/hour=$PREHOUR}/*"
JOIN_PATH="/user/$USER_NAME/ctr_space/ed_join_click_hour/join/${DATE}${HOUR}"
YESTDAY_JOIN_PATH="/user/$USER_NAME/ctr_space/ed_join_click_hour/join/${PREDATE}${PREHOUR}"
UNJOIN_PATH="/user/$USER_NAME/ctr_space/ed_join_click_hour/unjoin/${DATE}${HOUR}"

FLAG_PREFIX=flag_ods_usage_data_h_usage_naga_dsp
log_type="ed click"
for idx in `seq 0 1`;do
    PRETIME=`date -d "$idx hours ago $TIME" +%Y%m%d%H`
    date=${PRETIME:0:8}
    hour=${PRETIME:8:2}
    for tp in $log_type;do
        done_file="${FLAG_PREFIX}_${tp}_cn##${date}${hour}"
        loop_check $done_file 360 2
    done
done

#$HADOOP_HOME/bin/hadoop fs -rm -r -f $JOIN_PATH $YESTDAY_JOIN_PATH $UNJOIN_PATH

current=`date "+%Y-%m-%d %H:%M:%S"`
timeStamp=`date -d "$current" +%s` 
start_ed_join_click=$((timeStamp*1000+10#`date "+%N"`/1000000))

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 5 \
                 --executor-memory 8g \
                 --executor-cores 8 \
                 --driver-memory 12g \
                 --conf spark.sql.shuffle.partitions=40 \
                 --conf spark.dynamicAllocation.enabled=false \
                 --conf spark.driver.maxResultSize=8G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.sql.broadcastTimeout=36000 \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/ed_join_click.py $ED_PATH $CLK_PATH $JOIN_PATH $YESTDAY_JOIN_PATH $UNJOIN_PATH ${DATE}${HOUR}
