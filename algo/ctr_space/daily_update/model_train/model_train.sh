#!/bash/bin
bash /home/ling.fang/kinit.sh
USER_NAME=ling.fang
[ $# -eq 2 ] && JOB_TAG=$2 || JOB_TAG=""
ROOT_PATH=`pwd`
HADOOP_SHITU_INS="/user/${USER_NAME}/ctr_space/model_train${JOB_TAG}/shitu/ins/"
LOCAL_SHITU_INS="$ROOT_PATH/shitu_ins/"
MODEL_PATH="$ROOT_PATH/model_path/"
MODEL_BK_PATH="$ROOT_PATH/model_bk/"
MODEL_VERSION="$ROOT_PATH/model_version/"

[ ! -e $MODEL_PATH ] && mkdir -p $MODEL_PATH
[ ! -e $MODEL_BK_PATH ] && mkdir -p $MODEL_BK_PATH
[ ! -e $LOCAL_SHITU_INS ] && mkdir -p $LOCAL_SHITU_INS
[ ! -e $MODEL_VERSION ] && mkdir -p $MODEL_VERSION

HADOOP_HOME="/usr/local/hadoop-2.6.3"
TODAY=`date -d "0 days ago" +%Y%m%d`
YESTERDAY=`date -d "1 days ago" +%Y%m%d`
DATE_LIST="{"
for idx in `seq 1 7`;do
    day=`date -d "${idx} days ago $TODAY" +%Y%m%d`
    DATE_LIST=${DATE_LIST}","${day}
done
DATE_LIST=$DATE_LIST"}"
echo $DATE_LIST

MODEL_VERSION=`date -d "0 days ago" +%Y%m%d%H%M%S`
$HADOOP_HOME/bin/hadoop fs -getmerge $HADOOP_SHITU_INS/$DATE_LIST/ $LOCAL_SHITU_INS/shitu_ins.${MODEL_VERSION}
rm $LOCAL_SHITU_INS/.shitu_ins.${MODEL_VERSION}.crc
shuf $LOCAL_SHITU_INS/shitu_ins.$MODEL_VERSION -o $LOCAL_SHITU_INS/shitu_ins_shuf.$MODEL_VERSION

[ -e ${MODEL_PATH}/lr_model.dat ] && mv $MODEL_PATH/lr_model.dat $MODEL_BK_PATH/lr_model.dat.${MODEL_VERSION} 
$ROOT_PATH/ftrl/bin/ftrl $LOCAL_SHITU_INS/shitu_ins_shuf.$MODEL_VERSION l1_reg=1 l2_reg=0 \
    model_out=${MODEL_PATH}/lr_model.dat save_aux=1 is_incre=0
if [[ $? -ne 0 ]];then
    echo "ftrl train error!"
    exit 1
fi

#python script/model_push_util.py $MODEL_VERSION conf/model_push.conf
#if [[ $? -ne 0 ]];then
#    echo "model push online error!"
#    exit 1
#fi

#python script/model_push_util.py $MODEL_VERSION conf/model_push_test.conf
#if [[ $? -ne 0 ]];then
#    echo "model push online error!"
#    exit 1
#fi
