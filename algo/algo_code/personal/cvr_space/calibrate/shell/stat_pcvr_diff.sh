file_time_flag=`date +%Y%m%d`
click_path=/home/ling.fang/ctr_space/hourly_update/log/click/
trans_path=/home/ling.fang/ctr_space/hourly_update/log/transform/

cat $click_path/click.${file_time_flag}* > data/click.log
cat $trans_path/transform.${file_time_flag}* > data/transform.log

python script/join_transform.py data/click.log data/transform.log
