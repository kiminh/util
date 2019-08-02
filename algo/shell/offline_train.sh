#!/usr/bin/env bash

out_put_path=/home/work/chenjing/shitu_log
adfea_path=/home/work/chenjing/bayesalgo/adfea
model_path=/home/work/chenjing/bayesalgo/ftrl/

echo "begin adfea train.."

cd $adfea_path
./bin/adfea ./conf/offline_train_run.conf

echo "begin adfea valid.."
./bin/adfea ./conf/offline_valid_run.conf

cd $model_path
./bin/train 500000 ./conf/offline_ftrl.conf