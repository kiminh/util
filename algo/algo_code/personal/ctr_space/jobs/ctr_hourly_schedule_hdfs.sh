#!/bash/bin
work_path=/home/ad_user/personal/ling.fang
HADOOP_HOME="/usr/local/hadoop-2.6.3"
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file

if [ $# -eq 2 ];then
    TIME=$1
    VERSION=$2
else
    TIME=`date -d " 1 hours ago " "+%Y%m%d%H"`
    VERSION=""
fi

DATE=${TIME:0:8}
HOUR=${TIME:8:10}

current=`date "+%Y-%m-%d %H:%M:%S"`
timeStamp=`date -d "$current" +%s` 
begin_ed_join_click=$((timeStamp*1000+10#`date "+%N"`/1000000))
#ed join click
cd $work_path/ctr_space/ed_join_click_hour && bash -x ed_join_click.sh $DATE $HOUR
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model" "ed_join_click_hour $DATE $HOUR error!"
    mv $work_path/ctr_space/jobs/log/log.txt $work_path/ctr_space/jobs/log/log.${TIME}.txt
    exit 1
fi

current=`date "+%Y-%m-%d %H:%M:%S"`
timeStamp=`date -d "$current" +%s`
end_ed_join_click=$((timeStamp*1000+10#`date "+%N"`/1000000))
begin_adfea=$((timeStamp*1000+10#`date "+%N"`/1000000))
cd $work_path/ctr_space/hourly_update/shitu_generate2/ && bash -x run.sh $TIME
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model" "shitu_generate $TIME error!"
    mv $work_path/ctr_space/jobs/log/log.txt $work_path/ctr_space/jobs/log/log.${TIME}.txt
    exit 1
fi

current=`date "+%Y-%m-%d %H:%M:%S"`
timeStamp=`date -d "$current" +%s` 
end_adfea=$((timeStamp*1000+10#`date "+%N"`/1000000))
begin_model_train=$((timeStamp*1000+10#`date "+%N"`/1000000))
cd $work_path/ctr_space/hourly_update/model_train/ && bash -x run_hdfs.sh $TIME $VERSION
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model" "model_train time: $TIME, ver: $VERSION error!"
    mv $work_path/ctr_space/jobs/log/log.txt $work_path/ctr_space/jobs/log/log.${TIME}.txt
fi
current=`date "+%Y-%m-%d %H:%M:%S"`
timeStamp=`date -d "$current" +%s` 
end_model_train=$((timeStamp*1000+10#`date "+%N"`/1000000))

#tmp data clean
yes_time_stamp=`date -d "1 days ago" +%Y%m%d`
rm $work_path/ctr_space/hourly_update/shitu_generate2/shitu/shitu_${yes_time_stamp}*

find $work_path/ctr_space/hourly_update/shitu_generate2/shitu/ -cmin +720 -exec rm -f {} \;
find $work_path/ctr_space/hourly_update/shitu_generate2/shitu_tmp/ -cmin +120 -exec rm -f {} \;
find $work_path/ctr_space/hourly_update/model_train/incre_adfea/ -cmin +120 -exec rm -f {} \;
find $work_path/ctr_space/hourly_update/model_train/model_bk/ -cmin +120 -exec $HADOOP_HOME/bin/hadoop fs -put {} /user/ad_user/ctr_space/hour_model_train/model_bk/ \;
find $work_path/ctr_space/hourly_update/model_train/model_bk/ -cmin +120 -exec rm -f {} \;
#find $work_path/ctr_space/jobs/log/ -cmin +2880 -exec rm -f {} \;
find $work_path/ctr_space/calibrate/data/ -cmin +2880 -exec rm -f {} \;

mv $work_path/ctr_space/jobs/log/log.txt $work_path/ctr_space/jobs/log/log.${TIME}.txt
