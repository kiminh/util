#!/bin/bash

pkg_name="com.zymobile.jewel.candy.cookie"

hadoop fs -getmerge /user/ling.fang/1/2/3/4/5/automated_targeting/matrix/$pkg_name/lookalike_result_online/json data.json
