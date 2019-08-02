#!/bin/bash

result=""
bash -x script/batch_train.sh
if [[ $? -ne 0 ]];then
  result = "batch train error, please check it!!!"
else
  result = "batch train success!!!"
fi

python notifier/dingding_notifier.py $result
