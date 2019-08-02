#!/bin/bash
file_time_flag=$(date -d "-0 hour"  +%Y%m%d%H)
echo $file_time_flag

bidder_machine=10.175.245.175
remote_machine=10.175.247.236

remote_filepath=/home/work/run_env/bayes_rtbkit/log
dest_file_path=/home/work/chenjing/data
pv_file_path=/home/work/chenjing/pv_log
con_file_path=/home/work/chenjing/conversion_data
out_put_path=/home/work/chenjing/shitu_log
adfea_path=/home/work/chenjing/bayesalgo/adfea/
fea_path=/home/work/chenjing/bayesalgo/
model_path=/home/work/chenjing/bayesalgo/ftrl/
filepath=$(cd "$(dirname "$0")"; pwd)

click_file=click.$file_time_flag.log
win_file=win.$file_time_flag.log
bid_file=bid.$file_time_flag.log

echo "get click log form machine"
scp work@$remote_machine:$remote_filepath/$click_file $dest_file_path

echo "get win log form machine"
scp work@$remote_machine:$remote_filepath/$win_file $dest_file_path

echo "get bid log form machine"
scp work@$remote_machine:$remote_filepath/$bid_file $dest_file_path

