#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
echo $file_time_flag

bidder_machine=10.175.245.175
remote_log_machine=10.175.247.236

remote_log_path=/home/work/run_env/bayes_rtbkit/log
dest_file_path=/home/work/chenjing/data
pv_file_path=/home/work/chenjing/pv_log
con_file_path=/home/work/chenjing/conversion_data
shitu_path=/home/work/chenjing/shitu_log
current_path=$(cd "$(dirname "$0")"; pwd)
done_file_path=/home/work/chenjing/done_path

if [ ! -d $done_file_path ]; then
	mkdir $done_file_path
fi

click_file=click.$file_time_flag.log
win_file=win.$file_time_flag.log
bid_file=bid.$file_time_flag.log

echo "get click log form machine"
scp work@$remote_log_machine:$remote_log_path/$click_file $dest_file_path

echo "get win log form machine"
scp work@$remote_log_machine:$remote_log_path/$win_file $dest_file_path

echo "get bid log form machine"
scp work@$remote_log_machine:$remote_log_path/$bid_file $dest_file_path

pv_file=pv.$file_time_flag.log
echo "get pv log form machine"
#scp work@$remote_log_machine:$remote_log_path/$pv_file $pv_file_path

echo "get file done"
all_temp_click=$current_path/all_temp_click
#join click and win & bid log
done_file_time_flag=$(date -d "-9 hour"  +%Y%m%d%H)
echo "done time flag $done_file_time_flag"

rm -rf $all_temp_click

for (( i=1 ; i<10 ; i++ )); do
	file_tag=$(date -d "-$i hour"  +%Y%m%d%H)
	if [ -s $dest_file_path/click.$file_tag.log ]; then
		cat $dest_file_path/click.$file_tag.log >> $all_temp_click
	fi
done;

rm -rf $shitu_path/shitu_$done_file_time_flag
bash join_win_click.sh $dest_file_path/bid.$done_file_time_flag.log $dest_file_path/win.$done_file_time_flag.log $all_temp_click $shitu_path/shitu_$done_file_time_flag

echo "done file $done_file_time_flag"

done_time_hour=$(date -d "-9 hour" +%H)
if [[ $done_time_hour -eq 23 ]]
then
	date_time=$(date -d "-9 hour"  +%Y%m%d)
	touch $shitu_path/done.$date_time
fi

all_temp_bid=$current_path/all_temp_bid
all_temp_shitu=$current_path/all_temp_shitu
all_temp_win=$current_path/all_temp_win

rm $all_temp_bid
rm $all_temp_shitu
rm $all_temp_win

for (( i=1 ; i<9 ; i++ )); do
	file_tag=$(date -d "-$i hour"  +%Y%m%d%H)
	if [ -s $dest_file_path/bid.$file_tag.log ];then
		cat $dest_file_path/bid.$file_tag.log >> $all_temp_bid
	fi
	if [ -s $dest_file_path/win.$file_tag.log ];then
		cat $dest_file_path/win.$file_tag.log >> $all_temp_win
	fi
done;

bash join_win_click.sh $all_temp_bid $all_temp_win $all_temp_click $all_temp_shitu

echo "join file done"

rm $all_temp_win $all_temp_click $all_temp_bid

current_shitu_log_temp=$shitu_path/current_shitu_log_temp
current_shitu_log=$shitu_path/current_shitu_log
current_click_log=$con_file_path/current_click_log
current_click_log_h5=$con_file_path/current_click_log_h5
#no new click do nothing,else
if [ -f $dest_file_path/$click_file ] ; then
	#merge all shitu.log
	cat $shitu_path/shitu_* > $current_shitu_log
	cat $all_temp_shitu >> $current_shitu_log
	touch $done_file_path/done.shitu.$file_time_flag
	python count_exp.py $all_temp_shitu
fi

#has new  click
if [ -f $dest_file_path/$click_file ] ; then
#	grep "^1" $current_shitu_log > $current_click_log
	awk -F '\001' '{if ($45 == "1" && $1 == "1") {print $0}}' $current_shitu_log > $current_click_log
fi

#has new  click
if [ -f $dest_file_path/$click_file ] ; then
#	grep "^1" $current_shitu_log > $current_click_log
	awk -F '\001' '{if ($45 == "2" && $1 == "1") {print $0}}' $current_shitu_log > $current_click_log_h5
fi

cd $current_path

active_machine=121.41.91.127
act_remote_log_path=/home/work/callback_server/logs/active
act_dest_file_path=/home/work/chenjing/conversion_data
con_data=$act_dest_file_path/con_data.log

h5_act_remote_log_path=/home/work/callback_server/logs/active
h5_act_dest_file_path=/home/work/chenjing/h5_conversion_data
h5_con_data=$h5_act_dest_file_path/h5_con_data.log

echo "get conversion log form machine"
scp work@$active_machine:$act_remote_log_path/* $act_dest_file_path

temp_con_file=$act_dest_file_path/temp.dat
rm $temp_con_file
cat $act_dest_file_path/active.log* > $temp_con_file

awk -F '[\[\]]' '{if (NF >=4){print $2"\001"$4}}' $act_dest_file_path/* | sort | uniq > $temp_con_file
rm -rf $con_data
bash join_click_con.sh $current_click_log $temp_con_file $con_data
touch $done_file_path/done.conversion.$file_time_flag

scp work@$active_machine:$h5_act_remote_log_path/* $h5_act_dest_file_path
h5_temp_con_file=$h5_act_dest_file_path/temp.dat
rm $h5_temp_con_file
cat $h5_act_dest_file_path/active.log* > $h5_temp_con_file

awk -F '[\[\]]' '{if (NF >=4){print $2"\001"$4}}' $h5_act_dest_file_path/* | sort | uniq > $h5_temp_con_file
rm -rf $h5_con_data
bash join_click_con.sh $current_click_log_h5 $h5_temp_con_file $h5_con_data
touch $done_file_path/done.h5_conversion.$file_time_flag
