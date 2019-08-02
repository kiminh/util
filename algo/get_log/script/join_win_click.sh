#!/bin/bash

if [ $# != 4 ]
then
    echo join_win_click.sh need four params!
    echo usage: join_win_click.sh bid_log win_log click_log output_log
    exit -1
fi

bid_log_path=$1
win_log_path=$2
click_log_path=$3
output_log_path=$4

if [ -f $output_log_path ]
then
    echo output file "["$output_log_path"]" is already exist!
    echo please use another output
    exit -1
fi

awk -F '\t' '
BEGIN {
    while (getline < "'$click_log_path'" > 0) {
        click[$3"\t"$4] = 2;
    }
}
{
    if (click[$3"\t"$4] > 0) {
        print "1\t"$3"\t"$4"\t"$5
    } else {
        print "0\t"$3"\t"$4"\t"$5
    }

	win[$3"\t"$4] = 1;
}
END{
	for (i in click) {
		if (win[i] != 1) {
			print "1\t"i"\t999";
		}
	}
}' $win_log_path |cat > ${output_log_path}.temp

echo win log join complete!

awk -F '[\t\1]' '
BEGIN {
    while (getline < "'$output_log_path'.temp" > 0) {
        click[$2"\t"$3] = $1;
        price[$2"\t"$3] = $4;
        exist[$2"\t"$3] = 2;
    }
}
{
    if (exist[$3"\t"$4] > 0) {
        printf("%s\1%s", click[$3"\t"$4], price[$3"\t"$4]);
        for (i = 1; i<=NF;i++) {
            printf("\1%s", $i);
        }
        printf("\n");
    }
}
' $bid_log_path |cat> $output_log_path

rm ${output_log_path}.temp

echo bid log join complete!
