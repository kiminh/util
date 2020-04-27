#!/bin/bash
bash /home/ad_user/kinit_ad_user.sh
HADOOP_HOME="/usr/local/hadoop-2.6.3"
TODAY=`date -d "0 days ago" +%Y%m%d`
YESTERDAY=`date -d "1 days ago" +%Y%m%d`

ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file
USER_NAME=ad_user
[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
LOCAL_SHITU_INS="$ROOT_DIR/shitu_ins/"

DAILY_MODEL="$ROOT_DIR/daily_model/"
DAILY_MODEL_PATH="$ROOT_DIR/daily_model_path/"
DAILY_MODEL_BK="$ROOT_DIR/daily_model_bk/"

[ ! -e $DAILY_MODEL ] && mkdir -p $DAILY_MODEL
[ ! -e $DAILY_MODEL_BK ] && mkdir -p $DAILY_MODEL_BK
[ ! -e $DAILY_MODEL_PATH ] && mkdir -p $DAILY_MODEL_PATH
[ ! -e $LOCAL_SHITU_INS ] && mkdir -p $LOCAL_SHITU_INS

SHITU_PATH="/user/${USER_NAME}/naga_interactive/ocpc/model_train${JOB_TAG}/shitu/ins/"
$HADOOP_HOME/bin/hadoop fs -cat $SHITU_PATH/$YESTERDAY/* > ${LOCAL_SHITU_INS}/shitu_ins
[ $? -ne 0 ] && { alarm "Naga Interactive DSP CVR Model" "hadoop fs cat error!"; exit 1; }

shuf ${LOCAL_SHITU_INS}/shitu_ins -o ${LOCAL_SHITU_INS}/shitu_ins.shuffle

[ -e ${DAILY_MODEL}/lr_model.dat.${YESTERDAY} ] && mv $DAILY_MODEL/lr_model.dat.${YESTERDAY} $DAILY_MODEL_BK/
./ftrl/bin/ftrl ${LOCAL_SHITU_INS}/shitu_ins.shuffle alpha=0.01 beta=1 l1_reg=1 l2_reg=0. \
           model_out=${DAILY_MODEL_PATH}/lr_model.dat.${YESTERDAY} save_aux=1 is_incre=0
[ $? -ne 0 ] && { alarm "Naga Interactive DSP CVR Model" "[$VERSION] model train error!"; exit 1; }

mv $DAILY_MODEL_PATH/lr_model.dat.${YESTERDAY} $DAILY_MODEL/lr_model.dat.${YESTERDAY}
rm $LOCAL_SHITU_INS/shitu_ins
rm $LOCAL_SHITU_INS/shitu_ins.shuffle

exit 0
