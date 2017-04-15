#!/bin/bash

for i in `seq 0 150`;do
    echo $i
    ./entornycheck ../../adfea/shitu.ins $i >> result.dat
done
