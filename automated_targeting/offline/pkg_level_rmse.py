import json

res_dict = {}
for raw_line in open("result.json"):
    line_json = json.loads(raw_line.strip("\n\r "))
    pkg_name = line_json['package_name']
    rating = float(line_json['rating'])
    prediction = float(line_json['prediction'])
    
    if pkg_name not in res_dict:
        res_dict[pkg_name] = {}
        res_dict[pkg_name]['rmse'] = 0
        res_dict[pkg_name]['cnt'] = 0
    res_dict[pkg_name]['rmse'] += (rating - prediction) ** 2
    res_dict[pkg_name]['cnt'] += 1

rmse = {}
for pkg_name, mse in res_dict.items():
    rmse[pkg_name] = mse['rmse']/mse['cnt']

rmse_sorted = sorted(rmse.items(), key=lambda x:x[1], reverse=True)
f_out = open('pkg_rmse.txt', 'w')
for pkg, rmse in rmse_sorted:
    f_out.write("%s\t%s\n" % (pkg, rmse))
