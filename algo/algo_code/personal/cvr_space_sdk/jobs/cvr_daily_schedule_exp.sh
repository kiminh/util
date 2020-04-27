#!/bash/bin
work_path=/home/ad_user/personal/ling.fang/
#ed join click
#cd $work_path/cvr_space/shitu_generate_exp && bash -x stat_user_state.sh
DES_PATH="/user/ad_user/user_stat/json/"
HADOOP_HOME="/usr/local/hadoop-2.6.3"
$HADOOP_HOME/bin/hadoop fs -cat $DES_PATH/* > $work_path/cvr_space/shitu_generate_exp/script/user_state.json

cd $work_path/cvr_space/shitu_generate_exp/
$HADOOP_HOME/bin/hadoop fs -cat /user/ad_user/ocpc/model_train/shitu/log/* > shitu.log

cat shitu.log | python $work_path/cvr_space/shitu_generate_exp/script/mapper_new.py | python $work_path/cvr_space/shitu_generate_exp/script/reducer_single.py > ../model_train_exp/shitu_ins/shitu_ins

cd $work_path/cvr_space/model_train_exp && bash -x model_train_daily_ftrl.sh
if [[ $? -ne 0 ]];then
    python utils/sms_sender.py "[cvr model hourly update]: model train error!"
    python utils/email_sender.py "[cvr model hourly update]: model train error!"
fi

