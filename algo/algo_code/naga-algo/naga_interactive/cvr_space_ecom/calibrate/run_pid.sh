#!/bash/bin
bash -x /home/ad_user/kinit_ad_user.sh
USER_NAME=ad_user
if [ $# -eq 1 ];then
    VERSION=$1
else
    echo "bash run_pid.sh version"
    exit 1
fi
HADOOP_HOME="/usr/local/hadoop-2.6.3"
HADOOP_PLAN_PATH="/user/$USER_NAME/naga_interactive/ocpc_ecom/click_join_trans/plan_path/"

ROOT_DIR=`pwd`
common_file="`pwd`/../../script/tools/common.sh"
source $common_file
[ $# -ne 1 ] && { alarm "Naga Interactive ECOM CVR Model Calibrate" "VERSION is null"; exit 1;}
VERSION=$1
USER_NAME=ad_user
HADOOP_HOME="/usr/local/hadoop-2.6.3"
[ ! -e $ROOT_DIR/data/ ] && mkdir $ROOT_DIR/data/
$HADOOP_HOME/bin/hadoop fs -cat $HADOOP_PLAN_PATH/$VERSION/* > $ROOT_DIR/data/plan_clc_trans_$VERSION.json
[ $? -ne 0 ] && { alarm "Naga Interactive ECOM CVR Model Calibrate" "hadoop cat data error!"; exit 1; }

python script/PIDControl_plan.py $ROOT_DIR/data/plan_clc_trans_$VERSION.json $VERSION
[ $? -ne 0 ] && { alarm "Naga Interactive ECOM CVR Model Calibrate" "run plan PID Conrol error!"; exit 1; }

python script/update_disc2redis.py $ROOT_DIR/data/plan_cali_${VERSION}.json
[ $? -ne 0 ] && { alarm "Naga Interactive ECOM CVR  Model Calibrate" "update discount to reids error!"; exit 1; }

exit 0
