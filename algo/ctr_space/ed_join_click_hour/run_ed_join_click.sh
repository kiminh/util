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

HADOOP_HOME="/usr/local/hadoop-2.6.3"
common_file="/home/ling.fang/script/tools/common.sh"
source $common_file
ROOT_DIR=`pwd`

if [ $# -eq 2 ];then
    DATE=$1
    HOUR=$2
    TIME="$DATE $HOUR"
else
    TIME=`date -d " 1 hours ago " "+%Y%m%d %H"`
    DATE=${TIME:0:8}
    HOUR=${TIME:9:2}
fi

for idx in `seq 0 1`;do
    PRETIME=`date -d "$idx hours ago $TIME" +%Y%m%d%H`
    date=${PRETIME:0:8}
    hour=${PRETIME:8:2}
    INPUTDIR="-input \"/data/external/dw/dw_usage_naga_dsp_ed_raw_d/dt=$date/hour=$hour/*/\" "${INPUTDIR}
    INPUTDIR="-input \"/data/external/dw/dw_usage_naga_dsp_click_raw_d/dt=$date/hour=$hour/*/\" "${INPUTDIR}
done    
echo $INPUTDIR
OUTPUTDIR="/user/${USER_NAME}/ctr_space/ed_join_click_hour/${DATE}${HOUR}/"

log_type="ed click"
for idx in `seq 0 1`;do
    PRETIME=`date -d "$idx hours ago $TIME" +%Y%m%d%H`
    date=${PRETIME:0:8}
    hour=${PRETIME:8:2}
    for tp in $log_type;do
        done_file="/data/external/dw/dw_usage_naga_dsp_${tp}_raw_d/dt=$date/hour=$hour/"
        loop_check $done_file 140 1
    done
done

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
        -Dmapreduce.job.queuename=ad \
        -Dmapred.map.tasks=500 \
        -Dmapred.reduce.tasks=50 \
        -Dmapred.job.map.capacity=500 \
        -Dmapred.job.reduce.capacity=50 \
        -Dmapred.job.name=${TASK_NAME}"_"${DATE}${HOUR} \
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
        -outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
        -file 'mapper.py' \
        -file 'reducer.py' \
        -mapper 'python mapper.py' \
        -reducer 'python reducer.py '${DATE}${HOUR} \
        $INPUTDIR \
        -output $OUTPUTDIR

    judge_result "hadoop_run"
}

hadoop_run

PRETIME=`date -d "1 hours ago $TIME" +%Y%m%d%H`
PREDATE=${PRETIME:0:8}
PREHOUR=${PRETIME:8:2}

JOINDIR="/user/${USER_NAME}/ctr_space/ed_join_click_hour/tmp/join/${DATE}${HOUR}/"
PREHOUR_JOINDIR="/user/${USER_NAME}/ctr_space/ed_join_click_hour/join/${PREDATE}${PREHOUR}/"
UNJOINDIR="/user/${USER_NAME}/ctr_space/ed_join_click_hour/unjoin/${DATE}${HOUR}/"

$HADOOP_HOME/bin/hadoop fs -test -e  ${JOINDIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${JOINDIR}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${JOINDIR}

$HADOOP_HOME/bin/hadoop fs -test -e  ${PREHOUR_JOINDIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${PREHOUR_JOINDIR}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${PREHOUR_JOINDIR}

$HADOOP_HOME/bin/hadoop fs -test -e  ${UNJOINDIR} && $HADOOP_HOME/bin/hadoop fs -rm -r ${UNJOINDIR}
$HADOOP_HOME/bin/hadoop fs -mkdir -p ${UNJOINDIR}

$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-A $JOINDIR && \
$HADOOP_HOME/bin/hadoop fs -touchz $JOINDIR/_SUCCESS && \
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-B $UNJOINDIR
$HADOOP_HOME/bin/hadoop fs -touchz $UNJOINDIR/_SUCCESS && \
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-C $PREHOUR_JOINDIR
$HADOOP_HOME/bin/hadoop fs -touchz $PREHOUR_JOINDIR/_SUCCESS && \
$HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR && \
{ echo "task[$DATE] success!"; exit 0;} || { echo "hadoop mv hdfs file failed!"; python utils/email_sender.py "task[${TASK_NAME}_${DATE}] failed...." "$ROOT_DIR"; exit -1;}
