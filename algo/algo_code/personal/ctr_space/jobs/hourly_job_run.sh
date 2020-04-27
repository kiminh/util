#!/bash/bin

#check the model train process
batch_process=`ps -ef | grep "ctr_hourly_schedule_hdfs.sh" | grep -v grep`
while [ -n "$batch_process" ];do
  echo "batch training now..."
  sleep 30
  batch_process=`ps -ef | grep "ctr_hourly_schedule_hdfs.sh" | grep -v grep`
done

bash -x ctr_hourly_schedule_hdfs.sh > ./log/log.txt 2>&1
if [[ $? -ne 0 ]];then
    echo "ctr hourly sechedule error"
    exit 1
fi
