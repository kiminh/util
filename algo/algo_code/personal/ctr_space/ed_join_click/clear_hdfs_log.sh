#!/bash/bin
file_time_flag=`date -d " 0 days ago" +%Y%m%d`
reserve_file_time=`date -d " 30 days ago " +%Y%m%d`
reserve_hourly_file_time=`date -d " 168 hours ago " +%Y%m%d%H`
echo $file_time_flag

HADOOP_HOME="/usr/local/hadoop-2.6.3"
hdfs_log_path=/user/ad_user/ctr_space/

function clear_hourly_data()
{
    path=$1
    reverse_time=$2
    for i in `seq 1 20`;do
        time_flag=`date -d " $i hours ago $reserve_time" +%Y%m%d%H`
        $HADOOP_HOME/bin/hadoop fs -test -e $path/$time_flag
        if [[ $? -eq 0 ]];then
            echo "$time_flag"
            $HADOOP_HOME/bin/hadoop fs -rm -r -f $path/$time_flag
        fi
    done
}

function clear_daily_data()
{
    path=$1
    reserve_time=$2
    for i in `seq 1 20`;do
        time_flag=`date -d " $i days ago $reserve_time" +%Y%m%d`
        $HADOOP_HOME/bin/hadoop fs -test -e $path/$time_flag
        if [[ $? -eq 0 ]];then
            echo "$time_flag"
            $HADOOP_HOME/bin/hadoop fs -rm -r -f $path/$time_flag
        fi
    done
}

clear_daily_data $hdfs_log_path/ed_join_click/unjoin/ $reserve_file_time
clear_daily_data $hdfs_log_path/ed_join_click/join/ $reserve_file_time

clear_daily_data $hdfs_log_path/model_train/shitu/adfea $reserve_file_time
clear_daily_data $hdfs_log_path/model_train/shitu/ins $reserve_file_time
clear_daily_data $hdfs_log_path/model_train/shitu/statinfo $reserve_file_time

clear_daily_data $hdfs_log_path/model_train/shitu_shuffle $reserve_file_time
clear_daily_data $hdfs_log_path/model_train_exp/shitu_shuffle $reserve_file_time

clear_daily_data $hdfs_log_path/model_train_exp/shitu/adfea $reserve_file_time
clear_daily_data $hdfs_log_path/model_train_exp/shitu/ins $reserve_file_time
clear_daily_data $hdfs_log_path/model_train_exp/shitu/statinfo $reserve_file_time

clear_hourly_data $hdfs_log_path/ed_join_click_hour/unjoin/ $reserve_hourly_file_time
clear_hourly_data $hdfs_log_path/ed_join_click_hour/join/ $reserve_hourly_file_time
clear_hourly_data $hdfs_log_path/ed_join_click_exp_hour/join/ $reserve_hourly_file_time
clear_hourly_data $hdfs_log_path/ed_join_click_exp_hour/unjoin/ $reserve_hourly_file_time
clear_hourly_data $hdfs_log_path/hour_model_train/shitu/adfea/ $reserve_hourly_file_time
clear_hourly_data $hdfs_log_path/hour_model_train/shitu/ins/ $reserve_hourly_file_time
clear_hourly_data $hdfs_log_path/hour_model_train/shitu/statinfo/ $reserve_hourly_file_time
clear_hourly_data $hdfs_log_path/hour_model_train_exp/shitu/adfea/ $reserve_hourly_file_time
clear_hourly_data $hdfs_log_path/hour_model_train_exp/shitu/ins/ $reserve_hourly_file_time
clear_hourly_data $hdfs_log_path/hour_model_train_exp/shitu/statinfo/ $reserve_hourly_file_time

