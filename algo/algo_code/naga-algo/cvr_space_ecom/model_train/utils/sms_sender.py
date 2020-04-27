import json
import logging
import sys, os
import requests
from config import SMS_CONFIG

class SmsSender(object):
    """ SmsSender """

    def __init__(self, test, endpoint, phones):
        """ init sth"""
        self.test = test
        self.endpoint = endpoint

        # don't contain +86
        self.phones = '|'.join(phones.split(','))

    def send_sms(self, msg_content):
        """ send_sms """
        if self.test:
            return

        msg = {"type": "sms",
               "targets": self.phones,
               "content": msg_content,
               "multi_msg": True}

        data = json.dumps(msg)
        response = requests.post(self.endpoint, data)

        if response.status_code < 300:
            logging.info("sms sends successfully, %s" % msg_content)
        else:
            logging.info("sms sends failed, %s" % msg_content)



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: python sms_sender.py message "
        exit(1)
    print "no process in __main__, %s" % os.path.realpath(__file__)
    sms_sender_instance = SmsSender(False, SMS_CONFIG["endpoint"], SMS_CONFIG["sms_list"])
    sms_sender_instance.send_sms(sys.argv[1])
