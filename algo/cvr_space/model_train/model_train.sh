#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
echo $file_time_flag

USER_NAME=ling.fang
HADOOP_HOME="/usr/local/hadoop-2.6.3"
PLID_EDCLKDIR="/user/${USER_NAME}/ctr_space/plid_stat_info/json"
$HADOOP_HOME/bin/hadoop fs -cat $PLID_EDCLKDIR/* > script/plid_edclk.json

root_path=`pwd`
shitu_log=/home/ling.fang/cvr_space/log/shitu/shitu.log
shitu_ins=${root_path}/shitu_ins/shitu_ins
# feature extract
cat ${shitu_log} | python script/mapper.py | python script/reducer_single.py > ${shitu_ins}
if [[ $? -ne 0 ]];then
    echo "adfea error"
    exit 1
fi

shuf ${shitu_ins} -o ${shitu_ins}.shuffle
if [[ $? -ne 0 ]];then
    echo "shuffle error"
    cp ${shitu_ins}.fea ${shitu_ins}.shuffle
fi

model_file="./model/lr_model.dat"
./ftrl/bin/ftrl ${shitu_ins}.shuffle alpha=0.01 beta=1 l1_reg=1 l2_reg=0. \
           model_out=${model_file}.new save_aux=0 is_incre=0
if [[ $? -ne 0 ]];then
  echo "ftrl train error"
  exit 1
fi

mv ${model_file} ./model_bk/"lr_model.dat".${file_time_flag}
mv ${model_file}.new ${model_file}

exit 1

VERSION=`date -d "0 days ago" +%Y%m%d%H%M%S`
python script/model_push_util.py $VERSION conf/model_push.conf
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model push online $VERSION error!"
    python utils/email_sender.py "online model train" "model push online $VERSION error!" 
    exit 1
fi

python script/model_push_util.py $VERSION conf/model_push_test.conf
if [[ $? -ne 0 ]];then
    echo "model push online error!"
    #exit 1
fi
python utils/sms_sender.py "ftrl ctr model train success!"
exit 0
