#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
echo $file_time_flag

bidder_machine=10.175.245.175
remote_machine=10.175.247.236

remote_filepath=/home/work/run_env/bayes_rtbkit/log
dest_file_path=/home/work/chenjing/data
pv_file_path=/home/work/chenjing/pv_log
out_put_path=/home/work/chenjing/shitu_log
adfea_path=/home/work/chenjing/bayesalgo/adfea/
fea_path=/home/work/chenjing/bayesalgo/
model_path=/home/work/chenjing/bayesalgo/ftrl/

click_file=click.$file_time_flag.log
win_file=win.$file_time_flag.log
bid_file=bid.$file_time_flag.log

echo "get file done"
all_temp_click=./all_temp_click
#join click and win & bid log
done_file_time_flag=$(date -d "-12 hour"  +%Y%m%d%H)
echo "done time flag $done_file_time_flag"

rm -rf $all_temp_click

for (( i=1 ; i<13 ; i++ )); do
	file_tag=$(date -d "-$i hour"  +%Y%m%d%H)
	if [ -s $dest_file_path/click.$file_tag.log ]; then
		cat $dest_file_path/click.$file_tag.log >> $all_temp_click
	fi
done;

rm -rf $out_put_path/shitu_$done_file_time_flag
bash join_win_click.sh $dest_file_path/bid.$done_file_time_flag.log $dest_file_path/win.$done_file_time_flag.log $all_temp_click $out_put_path/shitu_$done_file_time_flag

echo "done file $done_file_time_flag"

done_time_hour=$(date -d "-12 hour" +%H)
if [[ $done_time_hour -eq 23 ]]
then
	date_time=$(date -d "-13 hour"  +%Y%m%d)
	touch $out_put_path/done.$date_time
fi
