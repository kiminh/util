#!/bash/bin
work_path=/home/ling.fang/
work_bk_path=/home/work/ling.fang/
#ed join click
cd $work_path/ctr_space/hourly_update/shitu_generate/ && bash -x script/join_click.sh 
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[ctr model hourly update]: shitu join error!"
    python utils/email_sender.py "[ctr model hourly update]: shitu join error!"
    exit 1
fi

#cd $work_path/ctr_space/hourly_update/model_train && bash -x run.sh
#if [[ $? -ne 0 ]];then
#    python utils/sms_sender.py "[ctr model hourly update]: model train error!"
#    python utils/email_sender.py "[ctr model hourly update]: model train error!"
#    exit 1
#fi

#tmp data clean
#shitu保存在ssd上最近6个小时
find $work_path/ctr_space/hourly_update/log/shitu/ -cmin +360 -exec mv {} $work_bk_path/ctr_space/hourly_update/log/shitu/ \;
#shitu_tmp保存在ssd上最近3个小时
find $work_path/ctr_space/hourly_update/log/shitu_tmp/ -cmin +120 -exec mv {} $work_bk_path/ctr_space/hourly_update/log/shitu_tmp/ \;
#ed保存在ssd上最近5个小时
find $work_path/ctr_space/hourly_update/log/ed/ -cmin +120 -exec mv {} $work_bk_path/ctr_space/hourly_update/log/ed/ \;
find $work_path/ctr_space/hourly_update/log/click/ -cmin +1440 -exec mv {} $work_bk_path/ctr_space/hourly_update/log/click/ \;

#cd $work_path/ctr_space/hourly_update/shitu_generate && bash -x kafka_log_check.sh

find $work_bk_path/ctr_space/hourly_update/log/shitu/ -cmin +1440 -exec rm -f {} \;
find $work_bk_path/ctr_space/hourly_update/log/ed/ -cmin +1440 -exec rm -f {} \;
find $work_bk_path/ctr_space/hourly_update/log/shitu_tmp/ -cmin +1440 -exec rm -f {} \;
find $work_bk_path/ctr_space/hourly_update/log/click/ -cmin +1440 -exec rm -f {} \;

#find $work_path/ctr_space/hourly_update/model_train/incre_adfea/ -cmin +120 -exec rm -f {} \;
#find $work_path/ctr_space/hourly_update/model_train/model_bk/ -cmin +1440 -exec rm -f {} \;
