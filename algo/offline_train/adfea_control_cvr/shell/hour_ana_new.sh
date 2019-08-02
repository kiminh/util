#!/bin/bash
time_begin=$2
time_end=$1
start_time_flag=$(date -d "-$time_begin hour"  +%Y%m%d%H)
end_time_flag=$(date -d "-$time_end hour"  +%Y%m%d%H)
file_time_flag=$(date -d "-1 hour" +%Y%m%d%H)

dest_file_path=/home/work/fangling/data
active_file=/home/work/data/bridge_log/batch_active/batch_active_log
click_file=${dest_file_path}/click.${file_time_flag}.log
win_file=${dest_file_path}/win.${file_time_flag}.log
bid_file=${dest_file_path}/bid.${file_time_flag}.log

remote_machine_ip=192.168.18.9
remote_log_path=/home/work/run_env/DEPLOY/BidMax/Logger/log
local_log_path=/home/work/data/click_data
join_file_tmp=./done_file.tmp
join_file=./done_file
rm -rf $join_file
rm -rf ${join_file_tmp}

:>${click_file}
:>${win_file}
:>${bid_file}

#scp remote log to local
i=${start_time_flag}
while (( i <= end_time_flag ));do
  #scp work@${remote_machine_ip}:${remote_log_path}/win.${i}.log ${dest_file_path}/win.${i}.log 
  #scp work@${remote_machine_ip}:${remote_log_path}/click.${i}.log ${dest_file_path}/click.${i}.log 
  #scp work@${remote_machine_ip}:${remote_log_path}/bid.${i}.log ${dest_file_path}/bid.${i}.log 
  cp ${local_log_path}/click_${i} ${dest_file_path}/click.${i}.log
  temp=`expr $i \% 100`
  if (( $temp < 23 ));then
    let i+=1
    else
      let i+=77
  fi

done

i=${start_time_flag}
while (( i <= end_time_flag ));do
    cat ${dest_file_path}/click.${i}.log >> ${click_file}
    #cat ${dest_file_path}/win.${i}.log >> ${win_file}
    #cat ${dest_file_path}/bid.${i}.log >> ${bid_file}
    temp=`expr $i \% 100`
    if (( $temp < 23 ));then
    let i+=1
    else
        let i+=77
    fi

done

#bash join_win_click.sh ${bid_file} ${win_file} ${click_file} ${join_file}
#awk -F'\001' '{ if($1 == 1) print $0 }' ${join_file} | grep -i "is_track_active\":\"1" > ${join_file}.click
#python join_action.py ${join_file}.click ${active_file} ${join_file_tmp}
python join_action.py ${click_file} ${active_file} ${join_file_tmp}
python count_exp_addarrive.py ${join_file_tmp}

rm -rf ${click_file}
rm -rf ${win_file}
rm -rf ${bid_file}
