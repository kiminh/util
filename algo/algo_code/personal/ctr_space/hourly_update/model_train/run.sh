#!/bash/bin

#check the model train process
batch_process=`ps -ef | grep "hour_model_train_new.sh" | grep -v grep`
if [ -n "$batch_process" ];then
  echo "batch training now..."
  exit 0
fi

bash -x hour_model_train_new.sh
if [[ $? -ne 0 ]];then
    echo "model train error"
    exit 1
fi
