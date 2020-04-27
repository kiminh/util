#!/bash/bin
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
work_path=/home/ad_user/personal/ling.fang/
#ed join click

cd $work_path/cvr_space/click_join_trans && bash -x merge_hdfs_file.sh
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CVR Model Daily Update" "[cvr model daily update]: merge hdfs file error!"
    exit 1
fi

cd $work_path/cvr_space/click_join_trans && bash -x run_click_join_trans_new.sh 
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CVR Model Daily Update" "[cvr model daily update]: ed join click join error!"
    exit 1
fi

cd $work_path/cvr_space/shitu_generate && bash -x shell/stat_app_clctrans.sh
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CVR Model Daily Update" "[cvr model daily update]: stat app clctrans error!"
    exit 1   
fi

cd $work_path/cvr_space/shitu_generate && bash -x run_shitu.sh
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CVR Model Daily Update" "[cvr model daily update]: run shitu error!"
    exit 1
fi

cd $work_path/cvr_space/model_train && bash -x model_train_daily_ftrl.sh
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CVR Model Daily Update" "[cvr model daily update]: model train error!"
    exit 1
fi

