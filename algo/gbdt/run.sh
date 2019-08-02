#!/bin/bash

./bin/xgboost ./conf/machine.conf model_out=gbdt_model.dat

./bin/xgboost ./conf/machine.conf task=dump model_in=gbdt_model.dat name_dump=gbdt_model.json

./bin/gbdt_predict ./conf/gbdt.conf
