#!/bash/bin
work_path=/home/ad_user/personal/ling.fang/
HADOOP_HOME="/usr/local/hadoop-2.6.3"
cd $work_path/cvr_space_sdk/shitu_generate && \
$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ocpc/stat_app_cvr/json/* > script/app_clktrans.json

#ed join click
cd $work_path/cvr_space_sdk/shitu_generate && bash -x run_shitu.sh
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[cvr model hourly update]: run shitu error!"
    python utils/email_sender.py "[cvr model hourly update]: run shitu error!"
    exit 1
fi

cd $work_path/cvr_space_sdk/model_train && bash -x model_train.sh
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[cvr model hourly update]: model train error!"
    python utils/email_sender.py "[cvr model hourly update]: model train error!"
fi

