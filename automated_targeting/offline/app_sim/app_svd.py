from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import preprocessing

import sys
import json
import logging
DEFAULT_LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
DEFAULT_LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_LOG_LEVEL = 'INFO'
logging.basicConfig(format=DEFAULT_LOG_FORMAT,
                    datefmt=DEFAULT_LOG_DATEFORMAT,
                    level=DEFAULT_LOG_LEVEL)

apps = []
cates = []
read_more = []
for raw_line in open(sys.argv[1]):
    line = raw_line.strip("\n\r ")
    line_json = json.loads(line)
    apps.append(line_json["package_name"])
    read_more.append(line_json["read_more"])
    cates.append(line_json["category"])
logging.info("finished load apps data from disk.")

vectorizer = TfidfVectorizer(min_df=2,  
                             max_df=0.5, 
                             stop_words='english')
X = vectorizer.fit_transform(read_more)
svd = TruncatedSVD(100)
X = svd.fit_transform(X)
X_norm = preprocessing.normalize(X, norm='l2')
logging.info("finished LSA.")

print X.shape
f_out = open("apps_vector.json", 'w')
for app, cate, vec in zip(apps, cates, X.tolist()):
    res_json = {}
    res_json["pkg_name"] = app
    res_json["category"] = cate
    res_json["features"] = vec
    f_out.write(json.dumps(res_json))
    f_out.write("\n")
