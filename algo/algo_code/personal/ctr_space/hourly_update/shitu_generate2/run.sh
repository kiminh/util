#!/bash/bin
HADOOP_HOME="/usr/local/hadoop-2.6.3"
ROOT_DIR=`pwd`
BASICSIZE=$((1024*1024*1024))
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
    alarm "Naga CTR Model ShituGenerate" "$file_time_flag ed join click join error!"
    exit 1
fi
$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ctr_space/hour_model_train/shitu/ins/$file_time_flag/* > shitu_tmp/shitu_$file_time_flag

if [[ $(cat shitu_tmp/shitu_$file_time_flag | wc -l) -lt 1000000 ]];then
    alarm "Naga CTR Model ShituGenerate" "shitu_$file_time_flag data size smaller then 1000000!"
fi
bash -x run_shitu_hourly.sh $pre_file_time_flag
if [[ $? -ne 0 ]];then
    alarm "Naga CTR Model ShituGenerate" "$pre_file_time_flag ed join click join error!"
    exit 1
fi
$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ctr_space/hour_model_train/shitu/ins/$pre_file_time_flag/* > shitu/shitu_$pre_file_time_flag

filesize=`ls -l shitu_tmp/shitu_${file_time_flag} | awk '{ print $5 }'`
if [ $filesize -lt $BASICSIZE ]; then
    alarm "Naga CTR Model ShituGenerate" "shitu_$file_time_flag data size smaller than 1GB!"
fi

