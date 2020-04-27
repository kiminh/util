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
ROOT_PATH=`pwd`
JOB_PATH="$ROOT_PATH/"
SPARK_HOME="/usr/local/spark-2.1.1-bin-hadoop2.6"
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
for idx in `seq 1 60`;do
    day=`date -d "$idx days ago $DATE" +"%Y%m%d"`
    DATE_LST=${DATE_LST}","$day
done
DATE_LST=${DATE_LST}"}"

[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
ROOT_DIR=`pwd`

INPUTDIR="/user/${USER_NAME}/ocpc/click_join_trans/{temp/${DATE},stable/${DATE_LST}}/"
APPLIST_PATH="/user/james.jiang/1/2/3/4/5/dmp/batch/data_source_parse/user_behavior/applist/final/rst/cn/20191124/"
OUTPUTDIR="/user/${USER_NAME}/ocpc/click_join_trans_applist/${DATE}/"
done_file="/user/${USER_NAME}/ocpc/click_join_trans/temp/${DATE}/_SUCCESS"
loop_check $done_file 140 1

sparksubmit="${SPARK_HOME}/bin/spark-submit \
                 --master yarn \
                 --queue root.ad-root.etl.dailyetl.high \
                 --num-executors 50 \
                 --executor-memory 12g \
                 --driver-memory 12g \
                 --conf spark.dynamicAllocation.enabled=true \
                 --conf spark.dynamicAllocation.maxExecutors=120 \
                 --conf spark.dynamicAllocation.minExecutors=60 \
                 --conf spark.driver.maxResultSize=8G \
                 --conf spark.shuffle.service.enabled=true \
                 --conf spark.eventLog.enabled=true \
                 --conf spark.sql.broadcastTimeout=36000 \
                 --conf spark.eventLog.dir=$LOG_PATH"

$sparksubmit ${JOB_PATH}/script/join_applist.py $INPUTDIR $APPLIST_PATH $OUTPUTDIR
