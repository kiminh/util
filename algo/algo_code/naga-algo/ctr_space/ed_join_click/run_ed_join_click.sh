#!/bin/bash
bash /home/ad_user/kinit_ad_user.sh

function judge_result() { 
    if [[ $? -ne 0 ]];then
        echo $1" failed."
        exit -1
    else
        echo $1" success."
    fi  
}

USER_NAME="ad_user"
#YEAR=${DATE:0:4}

HADOOP_HOME="/usr/local/hadoop-2.6.3"
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file

if [ $# -eq 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi
YESTDAY=`date -d "1 days ago $DATE" +"%Y%m%d"`

for idx in `seq 0 1`;do
    day=`date -d "$idx days ago $DATE" +%Y%m%d`
    INPUTDIR="-input \"/data/external/ods/ods_usage_data_h/usage_type=usage_naga_dsp_ed/dt=$day/*/*/\" "${INPUTDIR}
    INPUTDIR="-input \"/data/external/ods/ods_usage_data_h/usage_type=usage_naga_dsp_click/dt=$day/*/*/\" "${INPUTDIR}
done    

echo $INPUTDIR
FLAG_PREFIX=flag_ods_usage_data_h_usage_naga_dsp
log_type="ed click"
for idx in `seq 0 1`;do
    day=`date -d "$idx days ago $DATE" +%Y%m%d`
    for tp in $log_type;do
        #done_file="${FLAG_PREFIX}_${tp}_raw_d_cn##$DATE"
        done_file="${FLAG_PREFIX}_${tp}_cn##$DATE"
        loop_check $done_file 140 2
    done
done
OUTPUTDIR="/user/${USER_NAME}/ctr_space/ed_join_click/${DATE}/"

OWNER_INFO="$USER_NAME@cootek.cn"
TASK_NAME="ed_join_click"

#-Dmapreduce.map.output.compress=false \
function hadoop_run() {
    $HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR
    $HADOOP_HOME/bin/hadoop jar /usr/local/hadoop-2.6.3/share/hadoop/tools/lib/hadoop-streaming-2.6.3.jar \
        -libjars  /home/ling.fang/script/tools/mr_plugins-1.0.jar \
        -D mapreduce.reduce.memory.mb=6144 \
        -D mapreduce.reduce.java.opts=-Xmx3276m \
        -Dmapred.map.tasks.speculative.execution=false \
        -Dmapred.reduce.tasks.speculative.execution=false \
        -Dstream.non.zero.exit.is.failure=false \
        -Dmapred.job.priority=HIGH \
        -Dmapreduce.job.queuename=root.ad-root.etl.dailyetl.high \
        -Dmapred.map.tasks=500 \
        -Dmapred.reduce.tasks=50 \
        -Dmapred.job.map.capacity=500 \
        -Dmapred.job.reduce.capacity=50 \
        -Dmapred.job.name=${TASK_NAME}"_"$DATE \
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

JOINDIR="/user/${USER_NAME}/ctr_space/ed_join_click/join/${DATE}/"
YESTDAY_JOINDIR="/user/${USER_NAME}/ctr_space/ed_join_click/join/${YESTDAY}/"
UNJOINDIR="/user/${USER_NAME}/ctr_space/ed_join_click/unjoin/${DATE}/"

$HADOOP_HOME/bin/hadoop fs -test -e  ${JOINDIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${JOINDIR}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${JOINDIR}

$HADOOP_HOME/bin/hadoop fs -test -e  ${YESTDAY_JOINDIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${YESTDAY_JOINDIR}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${YESTDAY_JOINDIR}

$HADOOP_HOME/bin/hadoop fs -test -e  ${UNJOINDIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${UNJOINDIR}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${UNJOINDIR}

$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-A $JOINDIR && \
$HADOOP_HOME/bin/hadoop fs -touchz $JOINDIR/_SUCCESS && \
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-B $UNJOINDIR
$HADOOP_HOME/bin/hadoop fs -touchz $UNJOINDIR/_SUCCESS && \
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-C $YESTDAY_JOINDIR
$HADOOP_HOME/bin/hadoop fs -touchz $YESTDAY_JOINDIR/_SUCCESS && \
$HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR && \
{ echo "task[$DATE] success!"; exit 0;} || { echo "hadoop mv hdfs file failed!"; python utils/email_sender.py "task[${TASK_NAME}_${DATE}] failed...." "$ROOT_DIR"; exit -1;}
