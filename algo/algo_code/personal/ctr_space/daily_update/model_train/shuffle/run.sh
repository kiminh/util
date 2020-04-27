#!/bin
bash /home/ling.fang/kinit_ad_user.sh
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
bash /home/ling.fang/kinit_ad_user.sh

if [ $# -ge 1 ];then
    DATE=$1
else
    DATE=`date -d " 0 days ago " +%Y%m%d`
fi

[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
ROOT_DIR=`pwd`

for i in `seq 1 7`;do
    day=`date -d " $i days ago $DATE" +%Y%m%d`
    INPUTDIR="-input \"/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/shitu/ins/${day}/\" "${INPUTDIR}
    done_file="/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/shitu/ins/${day}/_SUCCESS"
    loop_check $done_file 140 1
done

echo $INPUTDIR
OUTPUTDIR="/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/shitu_shuffle/${DATE}/"

OWNER_INFO="$USER_NAME@cootek.cn"
TASK_NAME="naga_ctr_generate_shitu_log"

function hadoop_run() {
    $HADOOP_HOME/bin/hadoop fs -rm -r $OUTPUTDIR
    $HADOOP_HOME/bin/hadoop jar /usr/local/hadoop-2.6.3/share/hadoop/tools/lib/hadoop-streaming-2.6.3.jar \
        -libjars  /home/ling.fang/script/tools/mr_plugins-1.0.jar \
        -D num.key.fields.for.partition=1 \
        -D mapreduce.map.memory.mb=4096 \
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
        -file 'mapper.py' \
        -file 'reducer.py' \
        -mapper 'python mapper.py' \
        -reducer 'python reducer.py'\
        $INPUTDIR \
        -output $OUTPUTDIR
        
    judge_result "hadoop_run"
}

hadoop_run

