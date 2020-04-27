#!/bash/bin
work_path="/home/work/ad_user/naga-algorithm/naga_interactive"
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file

if [ $# -eq 2 ];then
    TIME=$1
    VERSION=$2
else
    TIME=`date -d " 1 hours ago " "+%Y%m%d%H"`
    VERSION=""
fi

DATE=${TIME:0:8}
HOUR=${TIME:8:10}

function save_log() {
    mv $work_path/ctr2_space/jobs/log/log.txt $work_path/ctr2_space/jobs/log/log.${TIME}.txt
}

cd $work_path/ctr2_space/ed_join_click/ && job_run "shell/ed_join_click_hour.sh $TIME" 3 && \
[ $? -ne 0 ] && { alarm "Naga Interactive CTR2 Model Hourly Update" "ed join click join error!"; save_log; exit 1; }

cd $work_path/ctr2_space/shitu_generate && bash -x run_hourly.sh $TIME && \
[ $? -ne 0 ] && { alarm "Naga Interactive CTR2 Model Hourly Update" "run shitu error!"; save_log; exit 1; }

cd $work_path/ctr2_space/model_train/ &&  bash -x model_train_hourly.sh $TIME $VERSION && \
[ $? -ne 0 ] && { alarm "Naga Interactive CTR2 Model Hourly Update" "model train  error!"; save_log; exit 1; }

find $work_path/ctr2_space/shitu_generate/shitu/ -cmin +1440 -exec rm -f {} \;
find $work_path/ctr2_space/model_train/incre_adfea/ -cmin +120 -exec rm -f {} \;
find $work_path/ctr2_space/model_train/model_bk/ -cmin +2880 -exec rm -f {} \;
cd $work_path/ctr2_space/jobs && bash -x clear_hdfs_log.sh

save_log
