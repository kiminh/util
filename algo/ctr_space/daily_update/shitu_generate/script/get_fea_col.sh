#!/bash/bin

if [[ $# -ne 1 ]];then
    echo "Usage: ./get_fea_col.sh out_feature_col"
fi
head -1 ./script/data.json | python script/mapper.py | head -1 | awk -F'\t' '{print $2}' | awk -F' ' '{ for(i=1;i<=NF;i++) print $i}' | awk -F'\001' '{print $1}' > $1
