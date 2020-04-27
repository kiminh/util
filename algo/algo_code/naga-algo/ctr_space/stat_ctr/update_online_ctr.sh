#!/bash/bin

DATE=`date +"%Y%m%d%H%M%S"`
python update_online_ctr.py $DATE
cp model_push_tmp.conf model_push.conf
cp model_push_test_tmp.conf model_push_test.conf

echo "model_file = stat_ctr_$DATE.csv" >> model_push.conf
echo "model_file = stat_ctr_$DATE.csv" >> model_push_test.conf

python model_push_util.py $DATE model_push.conf
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "model push online $VERSION error!"
    python utils/email_sender.py "online model train" "model push online $VERSION error!" 
    exit 1
fi

python model_push_util.py $DATE model_push_test.conf

python utils/sms_sender.py "stat model push $VERSION success!"
