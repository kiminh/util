#!/bash/bin
bash /home/ling.fang/kinit_ad_user.sh
USER_NAME=ad_user
[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG="_exp"
ROOT_PATH=`pwd`
HADOOP_SHITU_INS="/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/shitu/ins/"
LOCAL_SHITU_INS="$ROOT_PATH/shitu_ins/"
#common_file="$ROOT_PATH/../../../script/tools/common.sh"
#source $common_file

MODEL="$ROOT_PATH/model/"
MODEL_PATH="$ROOT_PATH/model_path/"
MODEL_BK_PATH="$ROOT_PATH/model_bk/"
MODEL_VERSION="$ROOT_PATH/model_version/"
SHITU_SHUFFLE="/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/shitu_shuffle/"

[ ! -e $MODEL ] && mkdir -p $MODEL
[ ! -e $MODEL_PATH ] && mkdir -p $MODEL_PATH
[ ! -e $MODEL_BK_PATH ] && mkdir -p $MODEL_BK_PATH
[ ! -e $LOCAL_SHITU_INS ] && mkdir -p $LOCAL_SHITU_INS
[ ! -e $MODEL_VERSION ] && mkdir -p $MODEL_VERSION

HADOOP_HOME="/usr/local/hadoop-2.6.3"
TODAY=`date -d "0 days ago" +%Y%m%d`
YESTERDAY=`date -d "1 days ago" +%Y%m%d`

cd $ROOT_PATH/shuffle && bash -x shuffle.sh && cd -
$HADOOP_HOME/bin/hadoop fs -cat $SHITU_SHUFFLE/$TODAY/* > $LOCAL_SHITU_INS/shitu_ins

[ -e ${MODEL_PATH}/lr_model.dat.${YESTERDAY} ] && mv $MODEL_PATH/lr_model.dat.${YESTERDAY} $MODEL_BK_PATH/
$ROOT_PATH/ftrl/bin/ftrl $LOCAL_SHITU_INS/shitu_ins l1_reg=1 l2_reg=0 alpha=0.02 beta=1 \
    model_out=${MODEL_PATH}/lr_model.dat.${TODAY} save_aux=1 is_incre=0
if [[ $? -ne 0 ]];then
    #echo "ftrl train error!"
    alarm "Nage DSP CTR Model Daily Update" "[batch ctr model daily update error]"
    exit 1
fi
mv $MODEL_PATH/lr_model.dat.${TODAY} $ROOT_PATH/model/lr_model.dat.${TODAY}
#python utils/sms_sender.py "[batch ctr model daily update finished]"
#alarm "Nage DSP CTR Model Daily Update" "[batch ctr model daily update finished]"
rm $LOCAL_SHITU_INS/shitu_ins
exit 0
