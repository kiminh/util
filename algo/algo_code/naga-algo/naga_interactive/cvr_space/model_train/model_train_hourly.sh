#!/bin/bash
VERSION=`date +%Y%m%d%H%M%S`
if [ $# -ge 2 ];then
    DATE=$1
    VERSION=$2
else
    DATE=`date -d " 1 days ago " +%Y%m%d`
fi
[ $# -eq 3 ] && JOB_TAG=$3 || JOB_TAG=""

ROOT_DIR=`pwd`
USER_NAME="ad_user"
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file
HADOOP_HOME="/usr/local/hadoop-ha_new/"

MODEL_PATH="$ROOT_DIR/model"
MODEL_BK_PATH="$ROOT_DIR/model_bk"
LOCAL_SHITU_INS="$ROOT_DIR/shitu_ins"

[ ! -e $MODEL_PATH ] && mkdir -p $MODEL_PATH
[ ! -e $MODEL_BK_PATH ] && mkdir -p $MODEL_BK_PATH
[ ! -e $LOCAL_SHITU_INS ] && mkdir $LOCAL_SHITU_INS

TODAY=`date -d " 0 days ago " +%Y%m%d`
SHITU_INS="/user/${USER_NAME}/naga_interactive/ocpc/hour_model_train${JOB_TAG}/shitu/ins/$TODAY"
$HADOOP_HOME/bin/hadoop fs -cat $SHITU_INS/* > $LOCAL_SHITU_INS/shitu_ins
[ $? -ne 0 ] && { alarm "Nage Interactive DSP CVR Model" "$SHITU_INS not exist"; exit 1; }

shuf ${LOCAL_SHITU_INS}/shitu_ins -o ${LOCAL_SHITU_INS}/shitu_ins.shuffle

MODEL_FILE="./daily_model/lr_model.dat.$DATE"
[ ! -e $MODEL_FILE ] && alarm "Nage Interactive DSP CVR Model" "daily model $DATE not exist" && exit 1

MODEL_FILE_ONLINE="$MODEL_PATH/lr_model_online.dat"
./ftrl/bin/ftrl $LOCAL_SHITU_INS/shitu_ins.shuffle alpha=0.01 beta=1 l1_reg=1 l2_reg=0. \
           model_out=${MODEL_FILE_ONLINE}.new save_aux=1 is_incre=1 model_in=${MODEL_FILE}
[ $? -ne 0 ] && { alarm "Nage Interactive DSP CVR Model" "model train $VERSION error!"; exit 1; }

mv ${MODEL_FILE_ONLINE} $MODEL_BK_PATH/"lr_model_online.dat".${VERSION}
mv ${MODEL_FILE_ONLINE}.new ${MODEL_FILE_ONLINE}
python script/model_push_util.py $VERSION conf/model_push.conf
[ $? -ne 0 ] && { alarm "Nage DSP CVR Model" "[$VERSION] model push error!"; exit 1; }

exit 0
