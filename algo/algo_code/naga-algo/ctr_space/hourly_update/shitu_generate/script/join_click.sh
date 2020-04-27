#!/bash/bin

file_time_flag=$(date -d "-1 hour"  +%Y%m%d%H)
pre_file_time_flag=$(date -d "-2 hour"  +%Y%m%d%H)

data_path="/home/ling.fang/ctr_space/hourly_update/log/"
shitu_path="$data_path/shitu/"
shitu_path_tmp="$data_path/shitu_tmp/"
ed_path="$data_path/ed/"
click_path="$data_path/click/"

python script/join_click.py $ed_path/ed.${file_time_flag}.log \
    $click_path/click.${file_time_flag}.log $click_path/click.${pre_file_time_flag}.log $shitu_path_tmp/shitu_${file_time_flag}
#if [[ $? -ne 0 ]];then
#    exit 1
#fi

python script/join_click_tmp.py $shitu_path_tmp/shitu_${pre_file_time_flag} \
    $click_path/click.${file_time_flag}.log $shitu_path/shitu_${pre_file_time_flag}
if [[ $? -ne 0 ]];then
    exit 1
fi
