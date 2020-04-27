#!/bash/bin
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
pre_file_time_flag=`date -d "2 days ago" +%Y%m%d`
source $common_file
work_path=/home/work/ad_user/naga-algorithm/naga_interactive/

cd $work_path/ctr1_space/ed_join_click/ && job_run "shell/ed_join_click.sh" 3
[ $? -ne 0 ] && { alarm "Naga Interactive CTR1 Model Daily Update" "ed join click join error!"; exit 1;}

cd $work_path/ctr1_space/shitu_generate && bash -x shell/stat_tab_ctr.sh
[ $? -ne 0 ] && { alarm "Naga Interactive CTR1 Model Daily Update" "stat tab plid ctr!"; exit 1; }

cd $work_path/ctr1_space/shitu_generate && bash -x run_shitu_daily.sh
[ $? -ne 0 ] && { alarm "Naga Interactive CTR1 Model Daily Update" "run shitu error!"; exit 1;}

cd $work_path/ctr1_space/shitu_generate && bash -x run_shitu_daily.sh $pre_file_time_flag
[ $? -ne 0 ] && { alarm "Naga Interactive CTR1 Model Daily Update" "run shitu error!"; exit 1; }

cd $work_path/ctr1_space/model_train/ &&  bash -x model_train_daily.sh
[ $? -ne 0 ] && { alarm "Naga Interactive CTR1 Model Daily Update" "model train error!"; exit 1;}

cd $work_path/ctr1_space/jobs && bash -x clear_hdfs_log.sh
