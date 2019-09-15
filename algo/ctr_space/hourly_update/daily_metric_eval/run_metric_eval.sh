#!/bash/bin

if [[ $# -eq 1 ]];then
    file_time_flag=$1
else
    file_time_flag=`date -d "1 days ago" +%Y%m%d`
fi
/usr/local/hadoop-2.6.3/bin/hadoop fs -cat /user/ling.fang/ctr_space/ed_join_click_hour/join/${file_time_flag}*/* | python3 metric_eval.py ${file_time_flag}
