#!/bash/bin
bash -x /home/ad_user/kinit_ad_user.sh
file_time_flag=$(date -d " 1 hours ago "  +%Y%m%d%H)
time_flag=$(date +%Y%m%d%H)
hour=${time_flag:8:2}
if [[ $hour -eq 00 ]];then
    exit 1
fi
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file
USER_NAME=ad_user
HADOOP_HOME="/usr/local/hadoop-2.6.3"
VERSION=`date +%Y%m%d%H%M%S`
bash -x shell/stat_adstyle_edclk.sh
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CTR Calibrate" "stat_adstyle_edclk $file_time_flag error!"
    exit 1
fi
$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ctr_space/calibrate/$file_time_flag/* > data/ed_click_$file_time_flag.json
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CTR Calibrate" "hadoop cat data $file_time_flag error!"
    exit 1
fi
if [[ `cat data/ed_click_$file_time_flag.json | wc -l` -eq 0 ]];then   
    alarm "Nage DSP CTR Calibrate" "ed click $file_time_flag file is null!"
    exit 1
fi
python script/PIDControl.py data/ed_click_$file_time_flag.json $VERSION
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CTR Calibrate" "pid control $file_time_flag error!"
    exit 1
fi

python script/update_cali2redis.py data/cali_$VERSION.json
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CTR Calibrate" "update_cali2redis $file_time_flag error!"
    exit 1
fi
