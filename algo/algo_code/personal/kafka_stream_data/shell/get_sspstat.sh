#!/bash/bin
HADOOP_HOME="/usr/local/hadoop-2.6.3"
time_flag=`date -d " 1 hours ago " +%Y%m%d%H`
hour=${time_flag:8:10}
day=${time_flag:0:8}

done_file=/data/external/dw/dw_usage_naga_adx_sspstat_h/dt=$day/hour=$hour/*
$HADOOP_HOME/bin/hadoop fs -ls $done_file
while [[ $? -ne 0 ]];do
    sleep 30
    $HADOOP_HOME/bin/hadoop fs -ls /data/external/dw/dw_usage_naga_adx_sspstat_h/dt=$day/hour=
done
$HADOOP_HOME/bin/hadoop fs -cat /data/external/dw/dw_usage_naga_adx_sspstat_h/dt=$day/hour=$hour/* > temp.gz
gunzip temp.gz
cat temp | python script/get_sspstat.py
rm temp
