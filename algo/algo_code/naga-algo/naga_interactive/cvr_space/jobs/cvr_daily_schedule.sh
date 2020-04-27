#!/bash/bin
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file
JOB_TAG=""
work_path=/home/work/ad_user/naga-algorithm/naga_interactive/
#ed join click
cd $work_path/cvr_space${JOB_TAG}/shitu_generate && bash -x shell/get_shitu_log.sh
[ $? -ne 0 ] && { alarm "Nage DSP CVR Model Daily Update" "[cvr model daily update]: get shitu log error!"; exit 1; }

cd $work_path/cvr_space${JOB_TAG}/shitu_generate && bash -x run_shitu_daily.sh
[ $? -ne 0 ] && { alarm "Nage DSP CVR Model Daily Update" "[cvr model daily update]: run shitu error!"; exit 1; }

cd $work_path/cvr_space${JOB_TAG}/model_train && bash -x model_train_daily.sh
[ $? -ne 0 ] && { alarm "Nage DSP CVR Model Daily Update" "[cvr model daily update]: model train error!"; exit 1; }

cd $work_path/cvr_space${JOB_TAG}/jobs && bash -x clear_hdfs_log.sh
