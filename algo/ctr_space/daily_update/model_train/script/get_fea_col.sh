#!/bash/bin

if [[ $# -ne 2 ]];then
    echo "Usage: ./get_fea_col.sh data.json out_feature_col"
fi
head -1 $1 | python script/mapper.py | head -1 | awk -F'\t' '{print $2}' | awk -F' ' '{ for(i=1;i<=NF;i++) print $i}' | awk -F'#' '{print $1}' > $2
