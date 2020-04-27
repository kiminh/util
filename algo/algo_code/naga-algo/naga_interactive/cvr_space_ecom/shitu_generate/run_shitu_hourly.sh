#!/bin
function judge_result() { 
    if [[ $? -ne 0 ]];then
        echo $1" failed."
        exit -1
    else
        echo $1" success."
    fi  
}

USER_NAME="ad_user"
ROOT_DIR=`pwd`

HADOOP_HOME="/usr/local/hadoop-2.6.3"
#HADOOP_HOME="/usr/local/hadoop-ha_new/"
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file

if [ $# -ge 1 ];then
    DATE=$1
else
    DATE=`date -d " 0 days ago " +%Y%m%d`
fi
[ $# -eq 2 ] && VERSION=$2 || VERSION=""
[ $# -eq 3 ] && JOB_TAG=$3 || JOB_TAG=""

INPUTDIR="-input \"/user/$USER_NAME/naga_interactive/ocpc_ecom/click_join_trans/join_path/$VERSION\" "${INPUTDIR}
OUTPUTDIR="/user/${USER_NAME}/naga_interactive/ocpc_ecom/hour_model_train${JOB_TAG}/shitu/${DATE}/"
done_file="/user/$USER_NAME/naga_interactive/ocpc_ecom/click_join_trans/join_path/$VERSION"
loop_check $done_file 140 1

OWNER_INFO="$USER_NAME@cootek.cn"
TASK_NAME="naga_interactive_cvr_ecom_generate_shitu_log (hourly)"

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
        -Dmapred.reduce.tasks=10 \
        -Dmapred.job.map.capacity=50 \
        -Dmapred.job.reduce.capacity=10 \
        -Dmapred.job.name="${TASK_NAME}"_$DATE \
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
        -outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
        -mapper 'python script/mapper.py hourly' \
        -reducer 'python script/reducer.py'\
        $INPUTDIR \
        -output $OUTPUTDIR
        
    judge_result "hadoop_run"
}

hadoop_run

ADFEA_DIR=/user/${USER_NAME}/naga_interactive/ocpc_ecom/hour_model_train${JOB_TAG}/shitu/adfea/
ADFEA_PATH=$ADFEA_DIR/${DATE}/
INS_DIR=/user/${USER_NAME}/naga_interactive/ocpc_ecom/hour_model_train${JOB_TAG}/shitu/ins/
INS_PATH=$INS_DIR/${DATE}/
STATINFO_DIR=/user/${USER_NAME}/naga_interactive/ocpc_ecom/hour_model_train${JOB_TAG}/shitu/statinfo/
STATINFO_PATH=$STATINFO_DIR/${DATE}/
ACCEPT_PKG_DIR=/user/${USER_NAME}/naga_interactive/ocpc_ecom/hour_model_train${JOB_TAG}/shitu/acceptpkg/
ACCEPT_PKG_PATH=$ACCEPT_PKG_DIR/${DATE}/
APPCLKTRANS_DIR=/user/${USER_NAME}/naga_interactive/ocpc_ecom/hour_model_train${JOB_TAG}/shitu/appclktrans/
APPCLKTRANS_PATH=$APPCLKTRANS_DIR/${DATE}/
PLANCLKTRANS_DIR=/user/${USER_NAME}/naga_interactive/ocpc_ecom/hour_model_train${JOB_TAG}/shitu/planclktrans/
PLANCLKTRANS_PATH=$PLANCLKTRANS_DIR/${DATE}/

$HADOOP_HOME/bin/hadoop fs -test -e $ADFEA_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $ADFEA_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $ADFEA_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $INS_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $INS_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $INS_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $STATINFO_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $STATINFO_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $STATINFO_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $ACCEPT_PKG_DIR && $HADOOP_HOME/bin/hadoop fs -rm -r $APPCLKTRANS_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $APPCLKTRANS_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $ACCEPT_PKG_DIR && $HADOOP_HOME/bin/hadoop fs -rm -r $ACCEPT_PKG_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $ACCEPT_PKG_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $PLANCLKTRANS_DIR && $HADOOP_HOME/bin/hadoop fs -rm -r $PLANCLKTRANS_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $PLANCLKTRANS_PATH

$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-A $ADFEA_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $ADFEA_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-B $STATINFO_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $STATINFO_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-C $INS_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $INS_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-E $PLANCLKTRANS_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $PLANCLKTRANS_PATH/_SUCCESS

$HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR && \
{ mylog "task[$DATE] success!"; exit 0;} || { mylog "task[$DATE] failed!"; exit -1;}
