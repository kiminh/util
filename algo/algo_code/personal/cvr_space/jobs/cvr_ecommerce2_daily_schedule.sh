#!/bash/bin

work_path=/home/ad_user/ling.fang/
#ed join click
cd $work_path/cvr_space/shitu_generate && bash -x run_shitu_ecommerce2.sh
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[ecommerce2 cvr model daily update]: run shitu error!"
    python utils/email_sender.py "[ecommerce2 cvr model daily update]: run shitu error!"
    exit 1
fi


cd $work_path/cvr_space/model_train && bash -x model_ecommerce2_train_daily_ftrl.sh
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[ecommerce2 cvr model daily update]: model train error!"
    python utils/email_sender.py "[ecommerce2 cvr model daily update]: model train error!"
fi
