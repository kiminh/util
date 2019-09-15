#!/bin/bash

bash /home/ling.fang/kinit.sh

function judge_result() { 
    if [[ $? -ne 0 ]];then
        echo $1" failed."
        exit -1
    else
        echo $1" success."
    fi  
}

USER_NAME="ling.fang"
#YEAR=${DATE:0:4}

HADOOP_HOME="/usr/local/hadoop-2.6.3"
common_file="/home/ling.fang/script/tools/common.sh"
source $common_file
ROOT_DIR=`pwd`

if [ $# -eq 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi

# retention delay 1 day, so need last 8 days data  
for idx in `seq 0 7`;do
    day=`date -d "$idx days ago $DATE" +"%Y%m%d"`
    INPUTDIR="-input \"/data/external/dw/dw_usage_naga_dsp_transform_raw_d/dt=$day/*/*/\" "${INPUTDIR}
    INPUTDIR="-input \"/data/external/dw/dw_usage_naga_dsp_click_raw_d/dt=$day/*/*/\" "${INPUTDIR}
done

echo $INPUTDIR
OUTPUTDIR="/user/${USER_NAME}/ocpc/click_join_trans/${DATE}/"
#done_file="/user/dsp/ocpc/sdk_trans_preprocess_tp2/$DATE/_SUCCESS"
#loop_check $done_file 140 1

#done_file="/user/dsp/ocpc/wakeup_trans_process/install/$DATE/_SUCCESS"
#loop_check $done_file 140 1

FLAG_PREFIX=flag_dw_usage_naga_dsp
log_type="click transform"
for idx in `seq 0 1`;do
    day=`date -d "$idx days ago $DATE" +%Y%m%d`
    for tp in $log_type;do
        done_file="${FLAG_PREFIX}_${tp}_raw_d_cn##$DATE"
        loop_check $done_file 140 2
    done
done

#OWNER_INFO="$USER_NAME@cootek.cn"
TASK_NAME="click_join_trans"

#-Dmapreduce.map.output.compress=false \
#-reducer 'python reducer.py '${DATE} \
function hadoop_run() {
    $HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR
    $HADOOP_HOME/bin/hadoop jar /usr/local/hadoop-2.6.3/share/hadoop/tools/lib/hadoop-streaming-2.6.3.jar \
        -libjars  /home/ling.fang/script/tools/mr_plugins-1.0.jar \
        -D map.output.key.field.separator='#' \
        -D num.key.fields.for.partition=1 \
        -D mapreduce.reduce.memory.mb=6144 \
        -D mapreduce.reduce.java.opts=-Xmx3276m \
        -Dmapred.map.tasks.speculative.execution=false \
        -Dmapred.reduce.tasks.speculative.execution=false \
        -Dstream.non.zero.exit.is.failure=false \
        -Dmapred.job.priority=HIGH \
        -Dmapreduce.job.queuename=root.ad-root.etl.dailyetl.high \
        -Dmapred.map.tasks=500 \
        -Dmapred.reduce.tasks=20 \
        -Dmapred.job.map.capacity=500 \
        -Dmapred.job.reduce.capacity=20 \
        -Dmapred.job.name=${TASK_NAME}_$DATE \
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
        -outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
        -file 'mapper.py' \
        -file 'reducer.py' \
        -mapper 'python mapper.py' \
        -reducer 'python reducer.py '${DATE} \
        $INPUTDIR \
        -output $OUTPUTDIR

    judge_result "hadoop_run"
}

hadoop_run

one_day_ago=`date -d "1 days ago ${DATE}" +"%Y%m%d"`
STABLEDIR_ONEDAY_AGO="/user/${USER_NAME}/ocpc/click_join_trans/stable/${one_day_ago}/"
STABLEDIR="/user/${USER_NAME}/ocpc/click_join_trans/stable/${DATE}/"
TEMPDIR="/user/${USER_NAME}/ocpc/click_join_trans/temp/${DATE}/"
UNJOINDIR="/user/${USER_NAME}/ocpc/click_join_trans/unjoin/${DATE}/"
#UNJOINDIR_SDK_TRANS="/user/${USER_NAME}/ocpc/click_join_trans/unjoin/sdk_trans/${DATE}/"
#STAT_STABLE_DIR="/user/${USER_NAME}/ocpc/click_join_trans/statinfo/stable/${DATE}/"
#STAT_TEMP_DIR="/user/${USER_NAME}/ocpc/click_join_trans/statinfo/temp/${DATE}/"

$HADOOP_HOME/bin/hadoop fs -test -e  ${STABLEDIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${STABLEDIR}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${STABLEDIR} 

$HADOOP_HOME/bin/hadoop fs -test -e  ${STABLEDIR_ONEDAY_AGO} && $HADOOP_HOME/bin/hadoop fs -rm -r ${STABLEDIR_ONEDAY_AGO}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${STABLEDIR_ONEDAY_AGO} 

$HADOOP_HOME/bin/hadoop fs -test -e  ${TEMPDIR} && $HADOOP_HOME/bin/hadoop fs -rm -r  ${TEMPDIR}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${TEMPDIR} 

$HADOOP_HOME/bin/hadoop fs -test -e  ${UNJOINDIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${UNJOINDIR}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${UNJOINDIR}

#$HADOOP_HOME/bin/hadoop fs -test -e  ${UNJOINDIR_SDK_TRANS} && $HADOOP_HOME/bin/hadoop fs -rm -r ${UNJOINDIR_SDK_TRANS}
#$HADOOP_HOME/bin/hadoop fs -mkdir -p ${UNJOINDIR_SDK_TRANS}

#$HADOOP_HOME/bin/hadoop fs -test -e  ${STAT_STABLE_DIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${STAT_STABLE_DIR}
#$HADOOP_HOME/bin/hadoop fs -mkdir -p ${STAT_STABLE_DIR}

#$HADOOP_HOME/bin/hadoop fs -test -e  ${STAT_TEMP_DIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${STAT_TEMP_DIR}
#$HADOOP_HOME/bin/hadoop fs -mkdir -p ${STAT_TEMP_DIR}

$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-A $TEMPDIR && \
$HADOOP_HOME/bin/hadoop fs -touchz $TEMPDIR/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-B $STABLEDIR && \
$HADOOP_HOME/bin/hadoop fs -touchz $STABLEDIR/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-D $UNJOINDIR && \
$HADOOP_HOME/bin/hadoop fs -touchz $UNJOINDIR/_SUCCESS
#$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-D $STAT_TEMP_DIR && \
#$HADOOP_HOME/bin/hadoop fs -touchz $STAT_TEMP_DIR/_SUCCESS  && \
#$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-E $STAT_STABLE_DIR && \
#$HADOOP_HOME/bin/hadoop fs -touchz $STAT_STABLE_DIR/_SUCCESS  && \
#$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-F $UNJOINDIR_SDK_TRANS && \
#$HADOOP_HOME/bin/hadoop fs -touchz ${UNJOINDIR_SDK_TRANS}/_SUCCESS  && \
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-C $STABLEDIR_ONEDAY_AGO && \
$HADOOP_HOME/bin/hadoop fs -touchz ${STABLEDIR_ONEDAY_AGO}/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR

#clear_hadoop_data /user/${USER_NAME}/ocpc/click_join_trans/temp 7 $DATE && \
#clear_hadoop_data /user/${USER_NAME}/ocpc/click_join_trans/statinfo/temp 7 $DATE && \
#{ echo "task[$DATE] success!"; exit 0;} || { echo "hadoop mv hdfs file failed!"; \
#python utils/email_sender.py "task[${TASK_NAME}_${DATE}] failed...." "$ROOT_DIR"; exit -1;}
