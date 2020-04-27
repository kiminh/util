#!/bin
bash /home/ad_user/kinit_ad_user.sh
#kinit -kt /home/john.zhu/sven.fang.keytab sven.fang
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
common_file="/home/ling.fang/script/tools/common.sh"
source $common_file
bash /home/ad_user/kinit_ad_user.sh

if [ $# -ge 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi
DATE_YESTDAY=`date -d "1 days ago $DATE" +"%Y%m%d"`
DATE_LST="{"$DATE
for idx in `seq 1 7`;do
    day=`date -d "$idx days ago $DATE" +"%Y%m%d"`
    DATE_LST=${DATE_LST}","$day
done
DATE_LST=${DATE_LST}"}"

JOB_TAG="_exp"
ROOT_DIR=`pwd`

INPUTDIR="-input \"/user/${USER_NAME}/ocpc/click_join_trans/temp/${DATE}/\" "${INPUTDIR}
INPUTDIR="-input \"/user/${USER_NAME}/ocpc/click_join_trans/stable/${DATE_LST}/\" "${INPUTDIR}

#INPUTDIR="-input \"/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/applist/$DATE/\" "${INPUTDIR}
echo $INPUTDIR
OUTPUTDIR="/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/shitu/${DATE}/"

USER_STATE="/user/ad_user/user_stat/json/"
$HADOOP_HOME/bin/hadoop fs -cat $USER_STATE/* > script/user_state.json

done_file="/user/${USER_NAME}/ocpc/click_join_trans/temp/${DATE}/_SUCCESS"
loop_check $done_file 140 1

# get app data
#APP_DB="/data/dw/app_db/latest/json/"
#loop_check $APP_DB/_SUCCESS 140 1
#$HADOOP_HOME/bin/hadoop fs -cat $APP_DB/part* > app_db && \
#cat app_db |python script/parse_app_db.py > script/app_db.dat && rm app_db

OWNER_INFO="$USER_NAME@cootek.cn"
TASK_NAME="naga_cvr_generate_shitu_log"

#-D stream.map.output.field.separator='\t' \
#-D stream.num.map.output.key.fields=2 \
#-D map.output.key.field.separator='\t' \
#-D num.key.fields.for.partition=1 \
#-D map.output.key.field.separator='#' \
#-D num.key.fields.for.partition=1 \
#-D mapreduce.reduce.memory.mb=4096 \
#-D mapreduce.reduce.java.opts=-Xmx3276m \

function hadoop_run() {
    $HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR
    $HADOOP_HOME/bin/hadoop jar /usr/local/hadoop-2.6.3/share/hadoop/tools/lib/hadoop-streaming-2.6.3.jar \
        -libjars  /home/ling.fang/script/tools/mr_plugins-1.0.jar \
        -files 'script' \
        -D map.output.key.field.separator='#' \
        -D num.key.fields.for.partition=1 \
        -D mapreduce.map.memory.mb=8192 \
        -D mapreduce.map.java.opts=-Xmx3276m \
        -Dmapred.map.tasks.speculative.execution=false \
        -Dmapred.reduce.tasks.speculative.execution=false \
        -Dstream.non.zero.exit.is.failure=false \
        -Dmapred.job.priority=HIGH \
        -Dmapreduce.job.queuename=root.ad-root.etl.dailyetl.high \
        -Dmapred.map.tasks=1000 \
        -Dmapred.reduce.tasks=200 \
        -Dmapred.job.map.capacity=1000 \
        -Dmapred.job.reduce.capacity=200 \
        -Dmapred.job.name=${TASK_NAME}_$DATE \
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
        -outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
        -mapper 'python script/mapper_new.py' \
        -reducer 'python script/reducer.py'\
        $INPUTDIR \
        -output $OUTPUTDIR
        
    judge_result "hadoop_run"
}

hadoop_run

ADFEA_DIR=/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/shitu/adfea/
ADFEA_PATH=$ADFEA_DIR/${DATE}/
INS_DIR=/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/shitu/ins/
INS_PATH=$INS_DIR/${DATE}/
STATINFO_DIR=/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/shitu/statinfo/
STATINFO_PATH=$STATINFO_DIR/${DATE}/
APPCLKTRANS_DIR=/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/shitu/appclktrans/
APPCLKTRANS_PATH=$APPCLKTRANS_DIR/${DATE}/
SHITULOG_DIR=/user/${USER_NAME}/ocpc/model_train${JOB_TAG}/shitu/log/
SHITULOG_PATH=$SHITULOG_DIR/${DATE}/

$HADOOP_HOME/bin/hadoop fs -test -e $ADFEA_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $ADFEA_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $ADFEA_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $INS_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $INS_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $INS_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $STATINFO_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $STATINFO_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $STATINFO_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $APPCLKTRANS_PATH && $HADOOP_HOME/bin/hadoop fs -rm -r $APPCLKTRANS_PATH
$HADOOP_HOME/bin/hadoop fs -mkdir -p $APPCLKTRANS_PATH

$HADOOP_HOME/bin/hadoop fs -test -e $SHITULOG_DIR && $HADOOP_HOME/bin/hadoop fs -rm -r $SHITULOG_DIR
$HADOOP_HOME/bin/hadoop fs -mkdir -p $SHITULOG_DIR

$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-A $ADFEA_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $ADFEA_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-B $STATINFO_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $STATINFO_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-C $INS_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $INS_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-D $APPCLKTRANS_PATH && \
$HADOOP_HOME/bin/hadoop fs -touchz $APPCLKTRANS_PATH/_SUCCESS
$HADOOP_HOME/bin/hadoop fs -mv $OUTPUTDIR/part-*-E $SHITULOG_DIR && \
$HADOOP_HOME/bin/hadoop fs -touchz $SHITULOG_DIR/_SUCCESS

$HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR && \
{ mylog "task[$DATE] success!"; exit 0;} || { mylog "task[$DATE] failed!"; exit -1;}
