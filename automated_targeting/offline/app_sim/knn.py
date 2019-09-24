from sklearn.neighbors import KDTree
import json
import sys
import math
import numpy as np

def norm(vec):
	vec_norm = []
	square_sum = 0.0
	for v in vec:
		square_sum += v * v
	sqrt_sum = math.sqrt(square_sum)
	if sqrt_sum == 0:
		return vec
	return [ v / sqrt_sum for v in vec ]

X = []
apps = []
for i, raw_line in enumerate(open(sys.argv[1])):
	line_json = json.loads(raw_line.strip("\n\r "))
	norm_vec = line_json['features']
	X.append(norm_vec)
	apps.append(line_json['pkg_name'])
	if i % 10000 == 0:
		print i

X = np.array(X)
#print X.shape
tree = KDTree(X, metric='euclidean')
print "KDTree create successfully."
res_json = {}
for i, app in enumerate(apps, start=1):
	dist, ind = tree.query(X[i-1,:].reshape(1, -1), k=20)
	dist = dist.tolist()[0]
	ind = ind.tolist()[0]
	app_dict = {}
	for k, j in enumerate(ind):
		if app == apps[j]:
			continue

		app_dict[apps[j]] = dist[k]
	res_json[app] = app_dict
	if i % 10000 == 0:
		print i
json.dump(res_json, open("apps_similar.json", 'w'), indent=4)
