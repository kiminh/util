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

cd $work_path/ctr_space_exp/ed_join_click_hour && bash -x ed_join_click.sh $DATE $HOUR
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model" "ed_join_click_hour $DATE $HOUR error!"
    exit 1
fi


cd $work_path/ctr_space_exp/hourly_update/shitu_generate2/ && bash -x run.sh $TIME
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model" "shitu_generate $TIME error!"
    exit 1
fi

cd $work_path/ctr_space_exp/hourly_update/model_train/ && bash -x run_hdfs.sh $TIME $VERSION
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model" "model_train time: $TIME, ver: $VERSION error!"
fi
#tmp data clean
#shitu保存在ssd上最近6个小时
find $work_path/ctr_space_exp/hourly_update/shitu_generate2/shitu/ -cmin +1440 -exec rm -f {} \;
#shitu_tmp保存在ssd上最近3个小时
find $work_path/ctr_space_exp/hourly_update/shitu_generate2/shitu_tmp/ -cmin +120 -exec rm -f {} \;

find $work_path/ctr_space_exp/hourly_update/model_train/incre_adfea/ -cmin +120 -exec rm -f {} \;
find $work_path/ctr_space_exp/hourly_update/model_train/model_bk/ -cmin +240 -exec rm -f {} \;

log_time=`date +"%Y%m%d%H"`
mv $work_path/ctr_space_exp/jobs/log/log.txt $work_path/ctr_space_exp/jobs/log/log.${log_time}.txt
