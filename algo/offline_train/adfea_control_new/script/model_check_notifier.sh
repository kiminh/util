#!/bin/bash

check_result=./check_result/result
source $check_result

zabbix_sender -z 192.168.226.54 -s bridge-model-001 -k model_beta0 -o $model_beta0
zabbix_sender -z 192.168.226.54 -s bridge-model-001 -k model_keys -o $model_keys
zabbix_sender -z 192.168.226.54 -s bridge-model-001 -k model_weight_nzero -o $model_weight_nzero
zabbix_sender -z 192.168.226.54 -s bridge-model-001 -k model_loss -o $model_loss
zabbix_sender -z 192.168.226.54 -s bridge-model-001 -k model_weight_high -o $model_weight_high
zabbix_sender -z 192.168.226.54 -s bridge-model-001 -k model_weight_low -o $model_weight_low
