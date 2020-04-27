#!/bin
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

if [ $# -ge 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi

[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
ROOT_DIR=`pwd`

INPUTDIR="-input \"/user/$USER_NAME/naga_interactive/ctr1_space/ed_join_click/join/${DATE}/\" "${INPUTDIR}
OUTPUTDIR="/user/${USER_NAME}/naga_interactive/ctr1_space/model_train${JOB_TAG}/shitu/${DATE}/"
done_file="/user/$USER_NAME/naga_interactive/ctr1_space/ed_join_click/join/${DATE}/_SUCCESS"
loop_check $done_file 400 1

OWNER_INFO="$USER_NAME@cootek.cn"
TASK_NAME="naga DSP interactive: ctr1_generate_shitu_log"

function hadoop_run() {
    $HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR
    $HADOOP_HOME/bin/hadoop jar /usr/local/hadoop-2.6.3/share/hadoop/tools/lib/hadoop-streaming-2.6.3.jar \
        -libjars  /home/ling.fang/script/tools/mr_plugins-1.0.jar \
        -files 'script' \
        -D map.output.key.field.separator='#' \
        -D num.key.fields.for.partition=1 \
        -D mapreduce.map.memory.mb=4096 \
        -D mapreduce.map.java.opts=-Xmx3276m \
        -Dmapred.map.tasks.speculative.execution=false \
        -Dmapred.reduce.tasks.speculative.execution=false \
        -Dstream.non.zero.exit.is.failure=false \
        -Dmapred.job.priority=HIGH \
        -Dmapreduce.job.queuename=root.ad-root.etl.dailyetl.high \
        -Dmapred.map.tasks=50 \
        -Dmapred.reduce.tasks=20 \
        -Dmapred.job.map.capacity=50 \
        -Dmapred.job.reduce.capacity=20 \
        -Dmapred.job.name="${TASK_NAME}"_$DATE \
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
        -outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
        -mapper 'python script/mapper.py' \
        -reducer 'python script/reducer.py'\
        $INPUTDIR \
        -output $OUTPUTDIR
        
    judge_result "hadoop_run"
}

hadoop_run

ADFEA_DIR=/user/${USER_NAME}/naga_interactive/ctr1_space/model_train${JOB_TAG}/shitu/adfea/
ADFEA_PATH=$ADFEA_DIR/${DATE}/
INS_DIR=/user/${USER_NAME}/naga_interactive/ctr1_space/model_train${JOB_TAG}/shitu/ins/
INS_PATH=$INS_DIR/${DATE}/
STATINFO_DIR=/user/${USER_NAME}/naga_interactive/ctr1_space/model_train${JOB_TAG}/shitu/statinfo/
STATINFO_PATH=$STATINFO_DIR/${DATE}/

$HADOOP_HOME/bin/hadoop fs -test -e $ADFEA_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $ADFEA_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $ADFEA_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $INS_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $INS_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $INS_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $STATINFO_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $STATINFO_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $STATINFO_PATH

$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-A $ADFEA_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $ADFEA_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-B $STATINFO_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $STATINFO_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-C $INS_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $INS_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR && \
{ mylog "task[$DATE] success!"; exit 0;} || { mylog "task[$DATE] failed!"; exit -1;}
