#!/bash/bin
if [[ $# -ne 2 ]];then
    echo "Usage: bash run_matrix.sh country app_name"
fi
country=$1
app_name=$2
bash -x shell/get_seed_user.sh $country $app_name

bash -x shell/create_lookalike_sample.sh $country $app_name

bash -x shell/run_model_train.sh $country $app_name
