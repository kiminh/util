#!/bash/bin
bash -x /home/ad_user/kinit_ad_user.sh
file_time_flag=$(date -d "-1 days"  +%Y%m%d)
echo $file_time_flag

USER_NAME=ad_user
HADOOP_HOME="/usr/local/hadoop-2.6.3"
ROOT_PATH=`pwd`
KAFKA_DATA_PATH="${ROOT_PATH}/kafka/online_log"
VERSION=`date +%Y%m%d%H%M%S`
$HADOOP_HOME/bin/hadoop fs -cat /user/$USER_NAME/ocpc_ecom/model_train/shitu/planclktrans/$file_time_flag/* > calibrate.dat.bk
if [[ `cat calibrate.dat.bk |wc -l` -ne 0 ]];then   
    cp calibrate.dat.bk calibrate.dat
fi

python script/PIDControl_plan.py $KAFKA_DATA_PATH/click/ $KAFKA_DATA_PATH/transform/ $VERSION
python script/update_bias2redis.py data/plan_theta_${VERSION}.json calibrate.dat

find ./data/ -cmin +1440 -exec rm -f {} \;
find ${KAFKA_DATA_PATH}/click/ -cmin +1440 -exec rm -f {} \;
find ${KAFKA_DATA_PATH}/transform/ -cmin +1440 -exec rm -f {} \;
