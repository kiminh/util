#!/bash/bin
bash /home/ad_user/kinit_ad_user.sh
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file
USER_NAME=ad_user
[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
LOCAL_SHITU_INS="$ROOT_DIR/shitu_ins/"

MODEL="$ROOT_DIR/model/"
DAILY_MODEL="$ROOT_DIR/daily_model/"
DAILY_MODEL_BK="$ROOT_DIR/daily_model_bk/"
MODEL_PATH="$ROOT_DIR/model_path/"
MODEL_BK_PATH="$ROOT_DIR/model_bk/"
MODEL_VERSION="$ROOT_DIR/model_version/"
SHITU_SHUFFLE="/user/${USER_NAME}/naga_interactive/ctr1_space/model_train${JOB_TAG}/shitu_shuffle/"

[ ! -e $MODEL ] && mkdir -p $MODEL
[ ! -e $DAILY_MODEL ] && mkdir -p $DAILY_MODEL
[ ! -e $DAILY_MODEL_BK ] && mkdir -p $DAILY_MODEL_BK
[ ! -e $MODEL_PATH ] && mkdir -p $MODEL_PATH
[ ! -e $MODEL_BK_PATH ] && mkdir -p $MODEL_BK_PATH
[ ! -e $LOCAL_SHITU_INS ] && mkdir -p $LOCAL_SHITU_INS
[ ! -e $MODEL_VERSION ] && mkdir -p $MODEL_VERSION

HADOOP_HOME="/usr/local/hadoop-2.6.3"
TODAY=`date -d "0 days ago" +%Y%m%d`
YESTERDAY=`date -d "1 days ago" +%Y%m%d`

cd $ROOT_DIR/shuffle && bash -x shuffle.sh && cd -
$HADOOP_HOME/bin/hadoop fs -cat $SHITU_SHUFFLE/$TODAY/* > $LOCAL_SHITU_INS/shitu_ins

[ -e ${MODEL_PATH}/lr_model.dat.${YESTERDAY} ] && mv $MODEL_PATH/lr_model.dat.${YESTERDAY} $MODEL_BK_PATH/
$ROOT_DIR/bin/ftrl $LOCAL_SHITU_INS/shitu_ins l1_reg=1 l2_reg=0 alpha=0.01 beta=1 \
    model_out=${MODEL_PATH}/lr_model.dat.${TODAY} save_aux=1 is_incre=0
[ $? -ne 0 ] && { alarm "Naga Interactive CTR1 Model Daily Update" "ctr1 model daily train error!" ;exit 1; }

mv $MODEL_PATH/lr_model.dat.${TODAY} $DAILY_MODEL/lr_model.dat.${TODAY}
rm $LOCAL_SHITU_INS/shitu_ins
alarm "Naga Interactive CTR1 Model Daily Update" "ctr1 model daily train sucessfully"
exit 0
