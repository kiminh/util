#!/bin/bash

for line in `cat sim.txt`
do
    num=`grep $line data.json | wc -l`
    echo $line, $num
done
