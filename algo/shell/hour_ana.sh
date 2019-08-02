#!/bin/bash
file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
echo $file_time_flag

dest_file_path=/home/work/chenjing/data
click_file=click.$file_time_flag.log
win_file=win.$file_time_flag.log
bid_file=bid.$file_time_flag.log

join_file=./done_file
rm -rf $join_file

bash join_win_click.sh $dest_file_path/bid.$file_time_flag.log $dest_file_path/win.$file_time_flag.log $dest_file_path/$click_file $join_file

echo "done file $file_time_flag"

python count_exp.py $join_file
