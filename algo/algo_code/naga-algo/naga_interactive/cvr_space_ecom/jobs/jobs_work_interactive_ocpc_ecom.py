import json
import logging
import os
import time
import threading
import Queue
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
logFilePath = './log/cvr_job_schedule.log'

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(logFilePath,
                                   when = 'd',
                                   interval = 1,
                                   backupCount=7)
formatter = logging.Formatter('[%(asctime)s-%(levelname)s - %(message)s]')
handler.setFormatter(formatter)
logger.addHandler(handler)

jobs_queue = Queue.Queue()

def jobs_handle():
    while True:
        if not jobs_queue.empty():
            job = jobs_queue.get()
            logger.info('Main  : job [%s] start.' % job)
            os.system(job)
            logger.info('Main  : job [%s] finished.' % job)

def jobs_in_queue():
    while True:
        time_flag = (datetime.now() + timedelta(hours = -1)).strftime("%Y%m%d%H")
        ver = time.strftime("%Y%m%d%H%M%S", time.localtime())
        hour = time_flag[0:10]
        minute = ver[10:12]
        if int(minute) == 9:
            job = 'bash -x cvr_hourly_schedule.sh %s %s >./log/log.txt 2>&1' % (hour, ver)
            jobs_queue.put(job)
            logger.info('put the job [%s] to queue.' % job)
        time.sleep(60)
        
def start_processor():
    logger.info("Main    : create and start jobs_in_queue thread.")
    t1 = threading.Thread(target=jobs_in_queue)
    t1.start()
   
    logger.info("Main    : create and start jobs_handle thread.")
    t2 = threading.Thread(target=jobs_handle)
    t2.start()

    logger.info("Main    : before joining thread.")
    t1.join()
    logger.info("Main    : thread done")
 
    logger.info("Main    : before joining thread.")
    t2.join()
    logger.info("Main    : thread done")

start_processor()
