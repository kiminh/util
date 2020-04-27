#!/bash/bin
ROOT_DIR=`pwd`
common_file="$ROOT_DIR/../../script/tools/common.sh"
source $common_file
TODAY=`date -d " 0 days ago " +"%Y%m%d"`
yestday_timestamp=`date -d " 1 days ago " +"%Y%m%d"`
pre_yestday_timestamp=`date -d " 2 days ago " +"%Y%m%d"`
JOB_TAG=""
work_path=/home/work/ad_user/naga-algorithm/naga_interactive/

LOG_PATH="$work_path/cvr_space_ecom${JOB_TAG}/jobs/log/"
[ ! -e $LOG_PATH ] && mkdir $LOG_PATH

if [ $# -eq 2 ];then
    TIME=$1
    VERSION=$2
else
    TIME=`date -d " 1 hours ago " "+%Y%m%d%H"`
    VERSION=`date -d " 1 hours ago " "+%Y%m%d%H%M%S"`
fi

function save_log() {
    mv $LOG_PATH/log.txt $LOG_PATH/log.${TIME}.txt
}

LATEST_MODEL_VER=0
for model_file in `ls $work_path/cvr_space_ecom${JOB_TAG}/model_train/daily_model/`;do
    file_basename=`basename $model_file`
    timeflag=${file_basename:0-8}
    if (( timeflag > last_time_stamp ));then
        LATEST_MODEL_VER=${timeflag}
    fi
done

if (( pre_yestday_timestamp > LATEST_MODEL_VER || LATEST_MODEL_VER == 0 ));then
    alarm "Nage Interactive DSP CVR Model${JOB_TAG} Hourly Update" "$yestday_timestamp cvr model has not already!"
fi

cd $work_path/cvr_space_ecom${JOB_TAG}/click_join_trans && bash -x click_join_trans_hour.sh $LATEST_MODEL_VER $VERSION
[ $? -ne 0 ] && { alarm "Nage Interactive DSP Ecom CVR Model${JOB_TAG} Hourly Update" "click join trans error!"; save_log; exit 1; }

cd $work_path/cvr_space_ecom${JOB_TAG}/shitu_generate && bash -x run_shitu_hourly.sh $TODAY $VERSION
[ $? -ne 0 ] && { alarm "Nage Interactive DSP Ecom CVR Model${JOB_TAG} Hourly Update" "run shitu hourly error!"; save_log; exit 1; }

cd $work_path/cvr_space_ecom${JOB_TAG}/model_train && bash -x merge_batch_online.sh $LATEST_MODEL_VER
[ $? -ne 0 ] && { alarm "Nage Interactive DSP Ecom CVR Model${JOB_TAG} Hourly Update" "merge batch online error!"; save_log; exit 1; }

cd $work_path/cvr_space_ecom${JOB_TAG}/model_train && bash -x model_train_hourly.sh $LATEST_MODEL_VER $VERSION
[ $? -ne 0 ] && { alarm "Nage Interactive DSP Ecom CVR Model${JOB_TAG} Hourly Update" "model train error!"; save_log; exit 1; }

cd $work_path/cvr_space_ecom/calibrate && bash -x run_pid.sh $VERSION > log/log.$VERSION.txt
[ $? -ne 0 ] && { alarm "Nage Interactive DSP Ecom CVR Model Hourly Update" "run PID Control error!"; save_log; exit 1; }

find $work_path/cvr_space_ecom${JOB_TAG}/model_train/model_bk/ -cmin +2880 -exec rm -f {} \;
save_log
