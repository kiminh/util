import json
import sys

model_log = {}
for raw in open(sys.argv[1]):
    line = raw.strip('\n\r ')
    
    line_json = json.loads(line)
    common_model_log = line_json['common_model_log']
    ad_model_log = line_json['ad_model_log']

    for key, value in common_model_log.items():
        model_log[key] = value
    for key, value in ad_model_log[0]['winad_model_info'].items():
        model_log[key] = value

for key, value in model_log.items():
    print key,value
