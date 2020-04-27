#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import json
import sys
"""

请求消息体： 请求消息中，Name, Supervisor， Title, Dept, Level 是必填项，其他是选填项。

           name: 报警处理人的域账号名字，只能填一个，   例如    xxx.da               (必填)
           supervisor: 监督人的域账号，只能填一个      例如    xxx.xiao             (必填）
           product: 报警所属的产品                                                 (选填)
           title: 报警title                                                       (必填)
           body:  报警正文                                                        (必填)
           dept:  所属部门                                                        (必填)
           level: 报警等级: P0、P1、P2、P3, 其中P3报警等级最低，
                           P0报警等级为最高。分别对应值为0，1，2，3      (必填)
           cb_done: 回调的url，当报警解除时，回调此网址               (选填)
           cb_done_arg: 回调的参数  # 字符串，可以写成json格式，来传递多个参数，
                       这个参数会通过post的方式传递给cb_done指定的url，key是arg，值就是cb_done_arg的值。
                       在http服务器中通过get_argument("arg")等类似方法获取到参数值
           location: 请求方服务所在的地区，[cn|us|eu|ap]，展示在zabbix大屏幕上时，会有所区分，再无他用  （选填)
"""

if len(sys.argv) < 3:
    print "Usage: python alarm.py title message"
    exit(1)

url = "http://cn-alarm.cootekservice.com"
msg = {
        "username": "ling.fang",
        "supervisor": "ling.fang",
        # "cc": "barry.li|tom.tao",
        # "product":"zabbix_ops_sfer",
        "product":"naga_dsp",
        "title": sys.argv[1],
        "body": sys.argv[2],
        "dept": "apd",
        "level": 3,
        "location": "cn",
        "zbx_trigger_id": "2422323"
        }
data = json.dumps(msg)
response = requests.post(url, data)
print response.status_code, response.text
