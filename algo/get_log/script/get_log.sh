#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
current_time_flag=$(date +%Y%m%d%H)
#active_time_flag=$(date -d "-1 hour"  +%Y-%m-%d_%H)
#active_time_flag=$(date -d "-1 hour"  +%Y-%m-%d_%H)

echo $file_time_flag

pid=$$

remote_log_machine=10.25.162.250

remote_log_path=/home/work/run_env/DEPLOY/BidMax/Logger/log
dest_file_path=/home/work/data/ori_data/

remote_action_machine=10.24.142.244
remote_action_path=/home/work/log/hig/active/


shitu_path="/home/work/data/shitu_log"

[[ ! -d ${shitu_path} ]] && mkdir ${shitu_path}
current_path=$(cd "$(dirname "$0")"; pwd)
done_file_path=/home/work/data/done_path



if [ ! -d $done_file_path ]; then
	mkdir $done_file_path
fi

done_file=${done_file_path}/shitu.done.${file_time_flag}
[[ -f ${done_file} ]] && echo "${done_file} exit " && exit 0

touch ${done_file}



#active.
click_file=click.$file_time_flag.log
win_file=win.$file_time_flag.log
bid_file=bid.$file_time_flag.log
active_file=active.$file_time_flag.log
active_file_new=active.$current_time_flag.log

echo "get click log form machine"
scp work@$remote_log_machine:$remote_log_path/$click_file $dest_file_path

echo "get win log form machine"
scp work@$remote_log_machine:$remote_log_path/$win_file $dest_file_path

echo "get bid log form machine"
scp work@$remote_log_machine:$remote_log_path/$bid_file $dest_file_path

#echo "get active log form machine"
#scp work@$remote_log_machine:$remote_log_path/$active_file $dest_file_path
#scp work@$remote_log_machine:$remote_log_path/$active_file_new $dest_file_path

pv_file=pv.$file_time_flag.log
echo "get pv log form machine"

echo "get file done"
done_file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)

done_file_pretime_flag=$(date -d "-2 hour"  +%Y%m%d%H)

echo "done time flag $done_file_time_flag"

rm -rf $all_temp_click


rm -rf $shitu_path/shitu_$done_file_time_flag

shitu_path_tmp="${shitu_path}_tmp"
[[ ! -d ${shitu_path_tmp} ]] && mkdir ${shitu_path_tmp}


rm -rf ${shitu_path_tmp}/shitu_$done_file_time_flag
bash join_win_click.sh $dest_file_path/bid.${done_file_time_flag}.log $dest_file_path/win.${done_file_time_flag}.log  $dest_file_path/click.${done_file_time_flag}.log ${shitu_path_tmp}/shitu_$done_file_time_flag
if [[ $? -ne 0 ]];then
    echo "join shitu log error"
    exit 1
fi


python join_click.py $dest_file_path/click.${done_file_time_flag}.log $shitu_path_tmp/shitu_${done_file_pretime_flag} $shitu_path/shitu_${done_file_pretime_flag}
if [[ $? -ne 0 ]];then
    echo "join laste click error"
    exit 1
fi

click_path="/home/work/data/click_log"
[[ ! -d ${click_path} ]] && mkdir ${click_path}
gawk -F'\001'  '$1==1 && $47==1' $shitu_path/shitu_${done_file_pretime_flag} > ${click_path}/click_${done_file_pretime_flag}

gawk -F'\001'  '$1==1 && $47==2' $shitu_path/shitu_${done_file_pretime_flag} > ${click_path}/h5_click_${done_file_pretime_flag}

#scp action
exit 0
remote_action_machine=10.24.142.244
remote_action_path=/home/work/log/hig/active/

action_file=active.${active_time_flag}.log
scp work@${remote_action_machine}:${remote_action_path}/${action_file} $dest_file_path


echo "done file $done_file_time_flag"


exit 0
