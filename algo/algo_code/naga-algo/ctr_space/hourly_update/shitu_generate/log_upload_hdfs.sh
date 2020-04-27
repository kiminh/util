#!/bash/bin
file_time_flag=`date -d " 0 days ago" +%Y%m%d00`
echo $file_time_flag

HADOOP_HOME="/usr/local/hadoop-2.6.3"
local_log_path=/home/ling.fang/ctr_space/hourly_update/log/
hdfs_log_path=/user/ling.fang/ctr_space/local_log/

for file in `ls $local_log_path/shitu/shitu_[0-9]*`;do
    file_basename=`basename $file` 
    timeflag=${file_basename:0-10}
    if (( timeflag < file_time_flag ));then
        $HADOOP_HOME/bin/hadoop fs -copyFromLocal $file $hdfs_log_path/shitu/
        if [[ $? -eq 0 ]];then
            rm -f $file
        fi
    fi  
done


for file in `ls $local_log_path/ed/ed\.[0-9]*`;do
    file_basename=`basename $file` 
    timeflag=${file_basename:3:10}
    echo $timeflag
    if (( timeflag < file_time_flag ));then
        $HADOOP_HOME/bin/hadoop fs -copyFromLocal $file $hdfs_log_path/ed/
        if [[ $? -eq 0 ]];then
            rm -f $file
        fi
        echo $file
    fi  
done

for file in `ls $local_log_path/click/click\.[0-9]*`;do
    echo $file
    file_basename=`basename $file` 
    timeflag=${file_basename:6:10}
    if (( timeflag < file_time_flag ));then
        $HADOOP_HOME/bin/hadoop fs -copyFromLocal $file $hdfs_log_path/click/
        if [[ $? -eq 0 ]];then
            rm -f $file
        fi
        echo $file
    fi  
done

#for file in `ls $local_log_path/shitu_tmp/shitu_[0-9]*`;do
#    echo $file
#    file_basename=`basename $file` 
#    timeflag=${file_basename:0-10}
#    if (( timeflag < file_time_flag ));then
#        echo $file
#    fi  
#done
