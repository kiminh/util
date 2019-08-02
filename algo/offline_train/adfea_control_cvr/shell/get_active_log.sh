#!/bin/bash

#2018-03-01_11
file_time_flag=$(date -d "-2 hour"  +%Y-%m-%d_%H)
bridge_machine=192.168.26.105
#remote_active_log_dir=/home/work/log/shuttle2/active
remote_active_log_dir=/home/work/log2/model/active
local_active_log_dir=/home/work/data/bridge_log/active
batch_active_log_file=/home/work/data/bridge_log/batch_active/batch_active_log

[[ ! -e $remote_active_log_dir ]] && mkdir -p $remote_active_log_dir
[[ ! -e $local_active_log_dir ]] && mkdir -p $local_active_log_dir

active_log_file=active.${file_time_flag}.log

scp work@${bridge_machine}:${remote_active_log_dir}/${active_log_file} ${local_active_log_dir}/${active_log_file}
if [[ $? -ne 0 ]];then
  echo "no active log!"
  touch ${local_active_log_dir}/${active_log_file}
fi

awk -F'auction\":\"' '{print $2}' ${local_active_log_dir}/${active_log_file} | awk -F'\",\"' '{print $1}' >> ${batch_active_log_file}

cp ${batch_active_log_file} ${batch_active_log_file}.bk
sort ${batch_active_log_file}.bk | uniq > ${batch_active_log_file}

file_time_flag=$(date -d "-2 hour"  +%Y%m%d%H)
click_log_dir=/home/work/data/click_data
click_log_file=${click_log_dir}/click_${file_time_flag}
conversion_dir=/home/work/data/conversion_data
conversion_log=conversion_log
batch_click_log=${click_log_dir}/batch_click_log

[[ ! -e $click_log_dir ]] && mkdir $click_log_dir
[[ ! -e $conversion_dir ]] && mkdir -p $conversion_dir

cat $click_log_file >> ${batch_click_log}

python shell/join_action.py ${batch_click_log} ${batch_active_log_file} ${conversion_file}
if [[ $? -ne 0 ]];then
  exit 1
fi

exit 0
