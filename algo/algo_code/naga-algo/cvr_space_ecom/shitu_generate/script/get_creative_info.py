import sys
import MySQLdb

db = MySQLdb.connect(host="data-mysql01.corp.cootek.com", user="dsp_rw", passwd="EgflwvmJ1lAY6lZS", port=3317, db="dsp_ad_info", charset='utf8')
cursor = db.cursor()
sql_str="select a.ad_id, ad_level, industry from dsp_ad_info a join dsp_organization_info b on (a.org_id=b.id)"
try:
    cursor.execute(sql_str)
    results = cursor.fetchall()
    for row in results:
        print "%s" % '\t'.join(str(x) for x in row)
except Exception as e:
    sys.stderr.write("Erroe: unable to fecth data, %s\n" % e)
db.close()
