#!/bin/bash

function alarm() {
    python /ssd/model_push/scripts/alarm.py "$1" "$2"
}

VERSION=`date -d "0 days ago" +%Y%m%d%H%M%S`
python /ssd/model_push/scripts/model_push_util.py $VERSION /ssd/model_push/scripts/model_push.conf
if [[ $? -ne 0 ]];then
    alarm "Nage DSP Hour Model" "model push online $VERSION error!"
    echo "Nage DSP Hour Model" "model push online $VERSION error!"

    exit 1
fi
alarm "Nage DSP Hour Model" "[$VERSION] ftrl ctr model train success! $bias_res"
echo "Nage DSP Hour Model" "[$VERSION] ftrl ctr model train success! $bias_res"

exit 0