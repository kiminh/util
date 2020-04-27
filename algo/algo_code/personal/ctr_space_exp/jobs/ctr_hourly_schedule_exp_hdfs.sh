#!/bash/bin
#work_path=/home/ad_user/personal/ling.fang
work_path=/home/work/jax.wang/personal/ling.fang
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
#cd $work_path/ctr_space_exp/ed_join_click_hour && bash -x ed_join_click.sh $DATE $HOUR 
#if [[ $? -ne 0 ]];then
#    alarm "Nage DSP Hour Model Exp" "ed_join_click_hour $DATE $HOUR error!"
#    mv $work_path/ctr_space_exp/jobs/log/log.txt $work_path/ctr_space_exp/jobs/log/log.${$TIME}.txt
#    exit 1
#fi


cd $work_path/ctr_space_exp/hourly_update/shitu_generate2/ && bash -x run.sh $TIME
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model Exp" "shitu_generate $TIME error!"
    mv $work_path/ctr_space_exp/jobs/log/log.txt $work_path/ctr_space_exp/jobs/log/log.${$TIME}.txt
    exit 1
fi


cd $work_path/ctr_space_exp/hourly_update/model_train/ && bash -x run_hdfs.sh $TIME $VERSION
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model Exp" "model_train time: $TIME, ver: $VERSION error!"
    mv $work_path/ctr_space_exp/jobs/log/log.txt $work_path/ctr_space_exp/jobs/log/log.${$TIME}.txt
fi

#tmp data clean
#shitu保存在ssd上最近6个小时
find $work_path/ctr_space_exp/hourly_update/shitu_generate2/shitu/ -cmin +720 -exec rm -f {} \;
#shitu_tmp保存在ssd上最近3个小时
find $work_path/ctr_space_exp/hourly_update/shitu_generate2/shitu_tmp/ -cmin +120 -exec rm -f {} \;

find $work_path/ctr_space_exp/hourly_update/model_train/incre_adfea/ -cmin +120 -exec rm -f {} \;
find $work_path/ctr_space_exp/hourly_update/model_train/model_bk/ -cmin +240 -exec $HADOOP_HOME/bin/hadoop fs -put {} /user/ad_user/ctr_space/hour_model_train_exp/model_bk/ \;
find $work_path/ctr_space_exp/hourly_update/model_train/model_bk/ -cmin +240 -exec rm -f {} \;

#log_time=`date +"%Y%m%d%H"`
mv $work_path/ctr_space_exp/jobs/log/log.txt $work_path/ctr_space_exp/jobs/log/log.${TIME}.txt
