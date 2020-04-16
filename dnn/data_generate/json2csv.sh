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

#HADOOP_HOME="/usr/local/hadoop-ha_new2/"
HADOOP_HOME=" /usr/local/hadoop-2.6.3/"
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../naga_interactive/script/tools/common.sh"
source $common_file

if [ $# -ge 1 ];then
    DATE=$1
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi

[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
ROOT_DIR=`pwd`

#train
done_file="/user/ad_user/naga_interactive/ocpc/shitu_log_with_applist/20200412/_SUCCESS"
INPUTDIR="-input \"/user/ad_user/naga_interactive/ocpc/shitu_log_with_applist/20200412/\" "${INPUTDIR} 
loop_check $done_file 140 1
OUTPUTDIR="/user/ad_user/naga_interactive/ocpc/shitu_log_with_applist_csv/20200412/"

#test
#done_file="/user/ad_user/naga_interactive/ocpc/shitu_log_with_applist/20200413/_SUCCESS"
#INPUTDIR="-input \"/user/ad_user/naga_interactive/ocpc/shitu_log_with_applist/20200413/\" "${INPUTDIR} 
#loop_check $done_file 140 1
#OUTPUTDIR="/user/ad_user/naga_interactive/ocpc/shitu_log_with_applist_csv/20200413/"


OWNER_INFO="$USER_NAME@cootek.cn"
TASK_NAME="naga DSP: convert_json2csv"

#hourly
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
        -Dmapred.map.tasks=500 \
        -Dmapred.reduce.tasks=256 \
        -Dmapred.job.map.capacity=500 \
        -Dmapred.job.reduce.capacity=256 \
        -Dmapred.job.name="${TASK_NAME}"_$DATE \
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
        -outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
        -mapper 'python script/mapper.py daily' \
        -reducer 'python script/reducer.py'\
        $INPUTDIR \
        -output $OUTPUTDIR
        
    judge_result "hadoop_run"
}

hadoop_run

{ mylog "task[$DATE] success!"; exit 0;} || { mylog "task[$DATE] failed!"; exit -1;}
