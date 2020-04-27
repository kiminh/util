import sys
import MySQLdb
import time
from datetime import datetime, date, timedelta

db = MySQLdb.connect(host="data-mysql01.corp.cootek.com", 
                     user="dsp_rw", 
                     passwd="EgflwvmJ1lAY6lZS", 
                     port=3317, db="dsp_ad_info", charset='utf8')
cursor = db.cursor()
#2019-11-11
temp_dt = datetime.strptime(time.strftime("%Y%m%d", time.localtime()), "%Y%m%d")
yestday = (temp_dt + timedelta(days = -1)).strftime("%Y-%m-%d")
sql_str="select plan_id, create_time from dsp_plan_info"
try:
    cursor.execute(sql_str)
    results = cursor.fetchall()
    for row in results:
        ct = row[1].strftime('%Y-%m-%d %H:%M:%S')
        if yestday in ct:
            print "%s" % row[0]
except Exception as e:
    sys.stderr.write("Error: unable to fecth data, %s\n" % e)
db.close()
