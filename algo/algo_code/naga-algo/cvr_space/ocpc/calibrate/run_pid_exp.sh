#!/bash/bin
bash -x /home/ad_user/kinit_ad_user.sh
file_time_flag=$(date -d "-1 days"  +%Y%m%d)
echo $file_time_flag
common_file="`pwd`/../../script/tools/common.sh"
source $common_file

USER_NAME=ad_user
HADOOP_HOME="/usr/local/hadoop-2.6.3"
KAFKA_DATA_PATH="/home/ad_user/personal/ling.fang/kafka_stream_data/data"
VERSION=`date +%Y%m%d%H%M%S`
$HADOOP_HOME/bin/hadoop fs -cat /user/$USER_NAME/ocpc/model_train/shitu/appclktrans/$file_time_flag/* > calibrate.dat.bk
if [[ `cat calibrate.dat.bk |wc -l` -ne 0 ]];then   
    cp calibrate.dat.bk calibrate.dat
fi

$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ocpc/stat_app_back_rate/$file_time_flag/part-* > stat_app_back_rate.json.bk
if [[ `cat stat_app_back_rate.json.bk |wc -l` -ne 0 ]];then
    cp stat_app_back_rate.json.bk stat_app_back_rate.json
fi

python script_exp/PIDControl_exp.py $KAFKA_DATA_PATH/click/ $KAFKA_DATA_PATH/transform/ $VERSION
if [[ $? -ne 0 ]];then
    alarm "CVR Model Calibrate" "PID Conrol error!"
    exit 1   
fi
python script_exp/PIDControl_plan_exp.py $KAFKA_DATA_PATH/click/ $KAFKA_DATA_PATH/transform/ $VERSION
if [[ $? -ne 0 ]];then
    alarm "CVR Model Calibrate" "PID plan Conrol error!"
    exit 1
fi

python script_exp/update_bias2redis_exp.py data/theta_${VERSION}.json data/plan_theta_${VERSION}.json calibrate.dat
if [[ $? -ne 0 ]];then
    alarm "CVR Model Calibrate" "update bias to reids error!"
    exit 1
fi
