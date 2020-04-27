#!/bash/bin
HADOOP_HOME="/usr/local/hadoop-2.6.3"
#HADOOP_HOME="/usr/local/hadoop-ha_new2/"
ROOT_DIR=`pwd`
USER_NAME=ad_user
JOB_TAG=""
common_file="$ROOT_DIR/../../../script/tools/common.sh"
source $common_file

if [ $# -eq 1 ];then
    file_time_flag=$1
else
    file_time_flag=`date -d " 1 hours ago " +%Y%m%d%H`
fi
bash -x run_shitu_hourly.sh $file_time_flag
if [[ $? -ne 0 ]];then
    alarm "Naga Interactive CTR2 ShituGenerate" "$file_time_flag run shitu hourly error!"
    exit 1
fi
[ ! -e shitu ] && mkdir shitu
$HADOOP_HOME/bin/hadoop fs -cat /user/${USER_NAME}/naga_interactive/ctr2_space/model_train_hour${JOB_TAG}/shitu/ins/$file_time_flag/* > shitu/shitu_$file_time_flag
