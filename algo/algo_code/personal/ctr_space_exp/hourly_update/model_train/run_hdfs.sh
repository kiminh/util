#!/bash/bin
if [[ $# -eq 2 ]];then
    TIME=$1
    VERSION=$2
fi
#check the model train process
#batch_process=`ps -ef | grep "hour_model_train_exp_hdfs.sh" | grep -v grep`
#while [ -n "$batch_process" ];do
#  echo "batch training now..."
#  sleep 30
#  batch_process=`ps -ef | grep "hour_model_train_exp_hdfs.sh" | grep -v grep`
#done

bash -x hour_model_train_exp_hdfs.sh $TIME $VERSION
if [[ $? -ne 0 ]];then
    echo "model train error"
    exit 1
fi
