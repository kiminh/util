#!/bash/bin
HADOOP_HOME="/usr/local/hadoop-2.6.3"
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../../script/tools/common.sh"
source $common_file

if [ $# -eq 1 ];then
    file_time_flag=$1
else
    file_time_flag=`date -d " 1 hours ago " +%Y%m%d%H`
fi
pre_file_time_flag=`date -d " 1 hours ago ${file_time_flag:0:8} ${file_time_flag:8:10} " +%Y%m%d%H`

bash -x run_shitu_hourly.sh $file_time_flag
if [[ $? -ne 0 ]];then
    alarm "Naga CTR Model ShituGenerate" "$file_time_flag run_shitu_hourly error!"
    exit 1
fi
$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ctr_space/hour_model_train/shitu/ins/$file_time_flag/* > shitu_tmp/shitu_$file_time_flag

bash -x run_shitu_hourly.sh $pre_file_time_flag
if [[ $? -ne 0 ]];then
    alarm "Naga CTR Model ShituGenerate" "$pre_file_time_flag run_shitu_hourly error!"
    exit 1
fi
$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ctr_space/hour_model_train/shitu/ins/$pre_file_time_flag/* > shitu/shitu_$pre_file_time_flag
