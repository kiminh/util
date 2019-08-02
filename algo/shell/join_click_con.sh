#!/bin/bash

if [ $# != 3 ]; then
    exit -1
fi

click_log_path=$1
con_log_path=$2
output_log_path=$3

if [ -f $output_log_path ]
then
    echo output file "["$output_log_path"]" is already exist!
    echo please use another output
    exit -1
fi

awk -F '[\t\1]' '
BEGIN {
    while (getline < "'$con_log_path'" > 0) {
        exist[$1] = 2;
    }
}
{
    if (exist[$5] > 0) {
		printf(1)
        for (i = 1; i<=NF;i++) {
            printf("\1%s", $i);
        }
        printf("\n");
    } else {
		printf(0)
		for (i = 1; i<=NF;i++) {
            printf("\1%s", $i);
		}
		printf("\n");
}
}
' $click_log_path |cat> $output_log_path
