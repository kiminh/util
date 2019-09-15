#!/bin/bash

#sleep 1h
start_dt=20190415
end_dt=20190504
for i in `seq 0 19`;do
    dt=`date -d "$i days $start_dt" +"%Y%m%d"`
    bash -x run_click_join_trans.sh $dt
    sleep 5m
done
