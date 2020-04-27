#!/bash/bin
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
work_path=/home/work/ling.fang/personal/ling.fang/

cd $work_path/ctr_space/ed_join_click_spark/ && bash -x shell/ed_join_click_dw.sh
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CTR Model Daily Update" "[ctr model daily update]: ed join click join error!"
    exit 1
fi

cd $work_path/ctr_space/daily_update/shitu_generate && bash -x run_shitu.sh
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CTR Model Daily Update" "[ctr model daily update]: run shitu error!"
    exit 1
fi

cd $work_path/ctr_space/daily_update/model_train/ &&  bash -x model_train_daily.sh
if [[ $? -ne 0 ]];then
    alarm "Nage DSP CTR Model Daily Update" "[ctr model daily update]: model train  error!"
    exit 1
fi

cd $work_path/ctr_space/ed_join_click/ && bash -x clear_hdfs_log.sh
