#!/bin/bash
# bash -x /home/ad_user/kinit_ad_user.sh
kinit -kt /home/ad_user/ad_user.keytab ad_user

file_time_flag=$(date -d "-1 days"  +%Y%m%d)
echo $file_time_flag

USER_NAME=ad_user
HADOOP_HOME="/usr/local/hadoop-2.6.3"

root_path=`pwd`
#shitu_log=/home/${USER_NAME}/cvr_space/log/shitu/shitu.log
shitu_log=$root_path/shitu/shitu.log
hdfs_shitu_ins_path=/user/${USER_NAME}/ocpc/model_train_ecommerce2/shitu/ins/
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

model_file="./model/lr_model.dat"
./ftrl/bin/ftrl ${shitu_ins}.shuffle alpha=0.05 beta=1 l1_reg=0.1 l2_reg=0. \
           model_out=${model_file}.new save_aux=1 is_incre=0
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model train $VERSION error!"
    exit 1
fi

mv ${model_file} ./model_bk/"lr_model.dat".${file_time_flag}
mv ${model_file}.new ${model_file}
$HADOOP_HOME/bin/hadoop fs -cat /user/$USER_NAME/ocpc/model_train_ecommerce2/shitu/planclktrans/$file_time_flag/* > ./model/calibrate.dat

VERSION=`date -d "0 days ago" +%Y%m%d%H%M%S`
python script/model_push_util.py $VERSION conf/model_ecommerce2.conf
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model push online $VERSION error!"
    python utils/email_sender.py "online model train" "model push online $VERSION error!" 
    exit 1
fi

python utils/sms_sender.py "[$VERSION] ftrl cvr model train success!"
exit 0
