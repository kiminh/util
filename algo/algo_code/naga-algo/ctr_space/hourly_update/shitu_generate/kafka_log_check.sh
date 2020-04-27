#!/bin/bash

file_time_flag=`date -d "1 hours ago" +%Y%m%d%H`
ed_data=/home/ling.fang/ctr_space/hourly_update/log/ed/ed.${file_time_flag}.log
ed_num=`wc -l ${ed_data} | awk '{print $1}'`
if [[ $ed_num -eq 0 ]];then
    python utils/sms_sender.py "kafka ed log error, please check!"
    python utils/email_sender.py "local log check" "kafka ed log error, please check!"    
fi
