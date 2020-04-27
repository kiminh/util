#!/bin/bash

kinit -kt /home/john.zhu/john.zhu.keytab john.zhu
path='/user/john.zhu/ocpc/checkpoint/'
HADOOP_HOME="/usr/local/hadoop-2.6.3"

${HADOOP_HOME}/bin/hadoop fs -rmr ${path}/*
