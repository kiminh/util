import json
import sys

fea_mapping = json.load(open(sys.argv[2]))
model_dict = json.load(open(sys.argv[1]))

coefficientMatrix = model_dict['coefficientMatrix']
interceptVector = model_dict['interceptVector']
values = coefficientMatrix['values']
features = fea_mapping['features']
beta0_value = interceptVector['values'][0]

with open(sys.argv[3], 'w') as f_out:
    for fea_index, fea_value in zip(features, values):
        f_out.write('%s %s\n' % (fea_index, fea_value))

    f_out.write('1430112322 %s\n' % (beta0_value))
