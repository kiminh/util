#!/bash/bin

bash /home/ling.fang/kinit_ad_user.sh

if [[ $# -eq 1 ]];then
    file_time_flag=$1
else
    file_time_flag=`date -d "1 days ago" +%Y%m%d`
fi
/usr/local/hadoop-2.6.3/bin/hadoop fs -cat /user/ad_user/ctr_space/ed_join_click/join/${file_time_flag}*/* | python3 metric_eval.py ${file_time_flag}
