# coding:utf-8

import requests
from bs4 import BeautifulSoup
import logging
import sys

ids = ['com.game.BMX_Boy','com.ccx.wallpape']
logging.captureWarnings(True)

if len(sys.argv) < 2:
    print "need two args!"
    exit(1)

fp_w = open(sys.argv[2],"w")

#for line in open(sys.argv[1]):
for line in ids:
    id = line.strip().split(" ")[0]
    url = 'https://play.google.com/store/apps/details?id={}'.format(id)
    wb_data = requests.get(url)
    soup = BeautifulSoup(wb_data.text, 'lxml')
    title = soup.title.text
    try:
        score = soup.select('div.score-container div.score')[0].text
        reviews_num = soup.select('div.reviews-stats span.reviews-num')[0].text
        classification = soup.select('a.document-subtitle.category span')[0].text
        appcarrier = soup.select('a.document-subtitle.primary span')[0].text
        update_time = soup.select('div.meta-info div.content')[0].text
        download_time = soup.select('div.meta-info div.content')[1].text
        version = soup.select('div.meta-info div.content')[2].text
    except IndexError:
        fp_w.write(str(id)+"\t" + str(score) + "\t" + str(reviews_num) + "\t" + "\t" + str(classification) + "\t" + str(update_time) + "\t" + str(download_time) + "\t" + str(version) + "\n")
        continue
    try:
        fp_w.write(str(id)+"\t" + str(score) + "\t" + str(reviews_num) + "\t" + "\t" + str(classification) + "\t" + str(update_time) + "\t" + str(download_time) + "\t" + str(version) + "\n")
    except UnicodeEncodeError:
        print id
        print score," " ,reviews_num," " ,classification

    print id
fp_w.close()
