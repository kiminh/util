#!/bash/bin
work_path=/home/ad_user/personal/ling.fang/
#ed join click
#cd $work_path/cvr_space/click_join_trans && bash -x run_click_join_trans_new.sh 
#if [[ $? -ne 0 ]];then
#    python utils/sms_sender.py "[cvr model hourly update]: click join transform join error!"
#    python utils/email_sender.py "[cvr model hourly update]: click join transform join error!"
#    exit 1
#fi

#cd $work_path/cvr_space/shitu_generate && bash -x shell/stat_app_clctrans.sh
#if [[ $? -ne 0 ]];then
#    exit 1   
#fi
cp $work_path/cvr_space/shitu_generate/script/app_clktrans.json $work_path/cvr_space/shitu_generate_exp/script/
cd $work_path/cvr_space/shitu_generate_exp && bash -x run_shitu.sh
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[cvr model hourly update]: run shitu error!"
    python utils/email_sender.py "[cvr model hourly update]: run shitu error!"
    exit 1
fi


cd $work_path/cvr_space/model_train_exp && bash -x model_train_daily_ftrl.sh
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[cvr model hourly update]: model train error!"
    python utils/email_sender.py "[cvr model hourly update]: model train error!"
fi

