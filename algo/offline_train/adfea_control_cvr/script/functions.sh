#g!/bin/bash
function scp_model()
{
    echo -e "local_model: $1"
    echo -e "dest_machine: $2"
    echo -e "dest_path: $3"
    echo -e "dest_model_name: $4"

    scp="scp -l 50000"
    model_file=$1
    bidder_machine_new=$2
    dest_path=$3
    dest_file=$4

    ${scp} ${model_file} ${bidder_machine_new}:${dest_path}/${dest_file}.new
 #   ssh work@${bidder_machine_new} "cd /home/work/model_monitor/ && bash -x script/check_model.sh ${dest_path}/${dest_file}.new"
    if [[ $? -ne 0 ]];then
        echo "Model update error!"
        return 1
    fi

    ssh work@${bidder_machine_new} "cd ${dest_path};
        [[ -e ${dest_file}.new ]] && cp ${dest_file} ${dest_file}.bk;
         mv ${dest_file}.new ${dest_file}" 
}
