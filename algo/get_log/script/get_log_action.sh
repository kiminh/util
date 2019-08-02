#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)


active_time_flag=$(date -d "-1 hour"  +%Y-%m-%d_%H)
current_time_flag=$(date -d "-0 hour"  +%Y-%m-%d_%H)

echo $file_time_flag

pid=$$

conversion_path="/home/work/data/conversion_log"
[[ ! -d ${conversion_path} ]] && mkdir ${conversion_path}
current_path=$(cd "$(dirname "$0")"; pwd)


done_file_path="/home/work/data/done_path"
done_file=${done_file_path}/action.done

source ${done_file}

#end_timestamp=2017010914
#click_path=/home/work/data/click_log/batch_click
#h5_click_path=/home/work/data/click_log/batch_click
#action_path=/home/work/data/conversion_log/batch_action
#conversion_path=/home/work/data/conversion_log/conversion.log
#h5_conversion_path=/home/work/data/conversion_log/h5_conversion.log

incre_h5_click=click_tmp_file_${file_time_flag}
new_end_time_stamp=${end_timestamp}
last_end_time_stamp=${end_timestamp}
for file in `ls /home/work/data/click_log/h5_click_[0-9]* | tac`;do
    timeflag=${file:0-10}
    if (( timeflag > last_end_time_stamp ));then
        cat ${file}  >> ${incre_h5_click}
        if (( timeflag > new_end_time_stamp ));then
            new_end_time_stamp=${timeflag}
        fi  
    else
        break
    fi  
done


remote_action_machine=10.24.142.244
remote_action_path=/home/work/log/hig/active/

echo "get active log form machine"

active_file=active.${active_time_flag}.log
active_file_new=active.${current_time_flag}.log
dest_file_path=/home/work/data/ori_data

scp work@$remote_action_machine:$remote_action_path/${active_file} $dest_file_path
scp work@$remote_action_machine:$remote_action_path/${active_file_new} $dest_file_path


cat $dest_file_path/${active_file} >> ${action_log}
cat $dest_file_path/${active_file_new} >> ${action_log}
cat ${incre_h5_click} >> ${h5_click_log}

cp ${h5_conversion_log} ${h5_conversion_log}.bk
#python join_action.py ${h5_click_log} ${action_log} ${h5_conversion_log}
python join_action_bidmax.py ${h5_click_log} ${action_log} ${h5_conversion_log}
if [[ $? -ne 0 ]];then
    echo "join action error!"
    exit 1
fi

cp ${done_file} ${done_file}.bk
echo "end_timestamp=${new_end_time_stamp}" > ${done_file}
echo "h5_click_log=${h5_click_log}" >> ${done_file}
echo "h5_conversion_log=${h5_conversion_log}" >> ${done_file}
echo "action_log=${action_log}" >> ${done_file}

rm -rf ${incre_h5_click}
exit 0
