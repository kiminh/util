#!/bash/bin
USER_NAME=ad_user/private_user/jax.wang
HADOOP_HOME="/usr/local/hadoop-2.6.3"
JOB_TAG="_exp"
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../../script/tools/common.sh"
source $common_file

if [ $# -eq 1 ];then
    file_time_flag=$1
else
    file_time_flag=`date -d " 1 hours ago " +%Y%m%d%H`
fi
pre_file_time_flag=`date -d " 1 hours ago ${file_time_flag:0:8} ${file_time_flag:8:10} " +%Y%m%d%H`

#bash -x run_shitu_hourly.sh $pre_file_time_flag
#if [[ $? -ne 0 ]];then
#    alarm "Naga CTR Model ShituGenerate" "$pre_file_time_flag ed join click join error!"
#    #python utils/sms_sender.py "[ctr model hourly update]: $pre_file_time_flag run shitu error!"
#    #python utils/email_sender.py "[ctr model hourly update]: $pre_file_time_flag run shitu error!"     
#    exit 1
#fi
#$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ctr_space/hour_model_train$JOB_TAG/shitu/ins/$pre_file_time_flag/* > shitu/shitu_$pre_file_time_flag

bash -x run_shitu_hourly.sh $file_time_flag
if [[ $? -ne 0 ]];then
    alarm "Naga CTR Model ShituGenerate" "$file_time_flag ed join click join error!"
    #python utils/sms_sender.py "[ctr model hourly update]: $file_time_flag run shitu error!"
    #python utils/email_sender.py "[ctr model hourly update]: $file_time_flag run shitu error!"
    exit 1
fi
#$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ctr_space/hour_model_train$JOB_TAG/shitu/ins/$file_time_flag/* > shitu_tmp/shitu_$file_time_flag
$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ctr_space/hour_model_train$JOB_TAG/shitu/ins/$file_time_flag/* > shitu/shitu_$file_time_flag

BASICSIZE=$((1024*1024*1024))
filesize=`ls -l shitu/shitu_${file_time_flag} | awk '{ print $5 }'`
if [ $filesize -lt $BASICSIZE ]; then
    alarm "Naga CTR Model ShituGenerate" "shitu_$file_time_flag data size smaller than 1GB!"
fi
