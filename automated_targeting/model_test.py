import json
import sys
def inner_product(vec1, vec2):
    res = 0
    for v1, v2 in zip(vec1, vec2):
        res += v1 * v2
    return res

vec = []
for raw_line in open("item.json"):
    line_json = json.loads(raw_line.strip("\n\r"))
    features = line_json['features']
    idx = line_json['id']
    if idx == int(sys.argv[1]):
        vec = [ float(v) for v in features ]
        break
print vec

user = {}
for i, raw_line in enumerate(open("user.json")):
    line_json = json.loads(raw_line.strip("\n\r"))
    features = line_json['features']
    idx = line_json['id']
    user[idx] = [ float(v) for v in features ]
    if i % 10000 == 0:
        print i

res = {}
for u, v in user.items():
    res[u] = inner_product(vec, v)
res_sort = sorted(res.items(), key=lambda k:k[1], reverse=True)
f_out = open(sys.argv[2], 'w')
for u, v in res_sort:
    f_out.write("%s\n" % (u))
f_out.close()
