for i in `seq 2 11`;do
    time_flag=`date -d " $i hours ago " +%Y%m%d%H`
    today_flag=`date +%Y%m%d`
    flag=${time_flag:0:8}
    if [[ $today_flag -ne $flag ]];then
        exit 0
    fi
    bash -x run_shitu_hourly.sh $time_flag
    /usr/local/hadoop-2.6.3/bin/hadoop fs -cat /user/ad_user/ctr_space/hour_model_train_exp/shitu/ins/${time_flag}/* > shitu/shitu_${time_flag}
done
