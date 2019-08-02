#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
echo $file_time_flag
done_file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
done_file_pretime_flag=$(date -d "-2 hour"  +%Y%m%d%H)
echo "done time flag $done_file_time_flag"

pid=$$

bidder_machine=192.168.18.9
remote_log_machine=192.168.18.9

remote_log_path=/home/work/run_env/DEPLOY/BidMax/Logger/log
dest_file_path=/home/work/model/data/jupitar2.0_data
shitu_path=/home/work/model/data/shitu_log2.0
current_path=$(cd "$(dirname "$0")"; pwd)
done_file_path=/home/work/model/data/done_path2.0
click_path=/home/work/model/data/click_data

[[ ! -e ${dest_file_path} ]] && mkdir -p ${dest_file_path}
[[ ! -e ${shitu_path} ]] && mkdir -p ${shitu_path}
[[ ! -e ${done_file_path} ]] && mkdir -p ${done_file_path}
[[ ! -e ${click_path} ]] && mkdir -p ${click_path}

if [ ! -d $done_file_path ]; then
	mkdir $done_file_path
fi
touch ${done_file}
click_file=click.$file_time_flag.log
win_file=win.$file_time_flag.log
bid_file=bid.$file_time_flag.log
echo "get click log form machine"
scp work@$remote_log_machine:$remote_log_path/$click_file $dest_file_path/
echo "get win log form machine"
scp work@$remote_log_machine:$remote_log_path/$win_file $dest_file_path/
echo "get bid log form machine"
scp work@$remote_log_machine:$remote_log_path/$bid_file $dest_file_path/
echo "get file done"

rm -rf $all_temp_click
rm -rf $shitu_path/shitu_$done_file_time_flag

shitu_path_tmp="${shitu_path}_tmp"
[[ ! -d ${shitu_path_tmp} ]] && mkdir ${shitu_path_tmp}

rm -rf ${shitu_path_tmp}/shitu_$done_file_time_flag
bash shell/join_win_click.sh $dest_file_path/bid.${done_file_time_flag}.log $dest_file_path/win.${done_file_time_flag}.log  $dest_file_path/click.${done_file_time_flag}.log ${shitu_path_tmp}/shitu_$done_file_time_flag.online
if [[ $? -ne 0 ]];then
    echo "join shitu log error"
    exit 1
fi

gawk -F'\001'  '$30!="180.168.253.2"'  ${shitu_path_tmp}/shitu_$done_file_time_flag.online > ${shitu_path_tmp}/shitu_$done_file_time_flag 

rm -rf ${shitu_path_tmp}/shitu_$done_file_time_flag.online
[[ ! -e ${shitu_path_tmp}/shitu_${done_file_pretime_flag} ]] && touch $shitu_path_tmp/shitu_${done_file_pretime_flag}

python shell/join_click.py $dest_file_path/click.${done_file_time_flag}.log $shitu_path_tmp/shitu_${done_file_pretime_flag} $shitu_path/shitu_${done_file_pretime_flag}
if [[ $? -ne 0 ]];then
    echo "join laste click error"
    exit 1
fi
awk -F'\001' '{ if($1 == 1) print $0 }' $shitu_path/shitu_${done_file_pretime_flag} | grep -i "is_track_active\":\"1" > $click_path/click_${done_file_pretime_flag}
echo "done file $done_file_time_flag"
exit 0
