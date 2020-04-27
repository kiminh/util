#!/bash/bin

cur_path=`pwd`
ROOT_PATH=$cur_path"/../"
#ed join click
cd $ROOT_PATH/shitu_generate && bash -x run_shitu.sh
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[ecommerce2 cvr model daily update]: run shitu error!"
    python utils/email_sender.py "[ecommerce2 cvr model daily update]: run shitu error!"
    exit 1
fi


cd $ROOT_PATH/model_train && bash -x model_train_daily_ftrl.sh
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[ecommerce2 cvr model daily update]: model train error!"
    python utils/email_sender.py "[ecommerce2 cvr model daily update]: model train error!"
fi
