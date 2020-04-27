#!/bin/bash
bash -x /home/ling.fang/kinit_ad_user.sh
USER_NAME=ad_user
file_time_flag=$(date -d "-1 days"  +%Y%m%d)
echo $file_time_flag

HADOOP_HOME="/usr/local/hadoop-2.6.3"

root_path=`pwd`
shitu_log=/home/ling.fang/cvr_space/model_train/shitu/shitu.log
hdfs_shitu_ins_path=/user/${USER_NAME}/ocpc/model_train/shitu/ins/
shitu_ins=${root_path}/shitu_ins/shitu_ins
$HADOOP_HOME/bin/hadoop fs -cat $hdfs_shitu_ins_path/$file_time_flag/* > ${shitu_ins}
if [[ $? -ne 0 ]];then
    echo "adfea error"
    exit 1
fi

shuf ${shitu_ins} -o ${shitu_ins}.shuffle
if [[ $? -ne 0 ]];then
    echo "shuffle error"
    cp ${shitu_ins}.fea ${shitu_ins}.shuffle
fi

VERSION=`date -d "0 days ago" +%Y%m%d%H%M%S`
model_file="./model/lr_model.dat"
mv ${model_file} ./model_bk/"lr_model.dat".${VERSION}

python script/fea_map.py ${shitu_ins}.shuffle
./bin/adpa_lbfgs ${shitu_ins}.shuffle.map l1_reg=0.45 lbfgs_stop_tol=1e-6 max_lbfgs_iter=200 model_out=${model_file}.new
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model train $VERSION error!"
    exit 1
fi
python script/convert_model.py ${model_file}.new ./dict/token2idx.json ${model_file}

$HADOOP_HOME/bin/hadoop fs -cat /user/$USER_NAME/ocpc/model_train/shitu/appclktrans/$file_time_flag/* > ./model/calibrate.dat
#exit 1
python script/model_push_util.py $VERSION conf/model_push.conf
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model push online $VERSION error!"
    python utils/email_sender.py "online model train" "model push online $VERSION error!" 
    exit 1
fi

python utils/sms_sender.py "[$VERSION] ftrl cvr model train success!"
exit 0
