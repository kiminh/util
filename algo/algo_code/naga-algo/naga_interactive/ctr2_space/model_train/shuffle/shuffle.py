import sys
import json
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.ml.linalg import Vectors
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: py ed_join_click.py" \
              " yesterday_ed yesterday_click befor_yes_ed befor_yes_click"
        sys.exit(0)

    in_path = sys.argv[1]
    out_path = sys.argv[2]
    DATE = sys.argv[3]

    sc = SparkContext(
        appName='naga DSP: shuffle_%s' % DATE
    )
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)

    data_df = spark.read.text(in_path)
    data_df.show()
    data_df = data_df.sample(False, 1.0, 2019)
    data_df.write.text(out_path)
