#!/bin/bash
file_time_flag=$(date -d "-1 days"  +%Y%m%d)
echo $file_time_flag

#check the model train process
batch_process=`ps -ef | grep "cvr_daily_schedule.sh" | grep -v grep`
if [ -n "$batch_process" ];then
  echo "batch training now..."
  exit 0
fi

cd ../shitu_generate/ && bash -x run_realtime_join.sh && cd -

root_path=`pwd`
shitu_ins=${root_path}/../shitu_generate/shitu_ins/shitu_ins
shuf ${shitu_ins} -o ${shitu_ins}.shuffle
if [[ $? -ne 0 ]];then
    echo "shuffle error"
    cp ${shitu_ins}.fea ${shitu_ins}.shuffle
fi

model_file="./model/lr_model.dat"
model_out="./model/lr_model_online.dat"
./ftrl/bin/ftrl ${shitu_ins}.shuffle alpha=0.03 beta=1 l1_reg=0.1 l2_reg=0. \
           model_out=${model_out}.new save_aux=1 is_incre=1 model_in=${model_file}
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model train $VERSION error!"
    exit 1
fi

mv ${model_out} ./model_bk/"lr_model_online.dat".${file_time_flag}
mv ${model_out}.new ${model_out}

VERSION=`date -d "0 days ago" +%Y%m%d%H%M%S`
python script/model_push_util.py $VERSION conf/model_push.conf
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model push online $VERSION error!"
    python utils/email_sender.py "online model train" "model push online $VERSION error!" 
    exit 1
fi

exit 0
