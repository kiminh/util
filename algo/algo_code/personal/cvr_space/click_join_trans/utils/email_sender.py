#!/usr/bin/env python
# -*- coding: utf-8 -*-

import email
import email.encoders
import email.mime.base
import email.mime.text
import logging
import smtplib
import time
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE

from config import MAIL_CONFIG


class MailSender(object):
    """ MysqlUtilHandler """

    def __init__(self, smtp_server=MAIL_CONFIG["SMTP_SERVER"], port=MAIL_CONFIG["EMAIL_PORT"],
                 user=MAIL_CONFIG["USER_NAME"], pwd=MAIL_CONFIG["PASSWORD"],
                 sender=MAIL_CONFIG["SENDER"], receiver_list=MAIL_CONFIG["EMAIL_RECEIVER"]):
        self.smtp_server = smtp_server
        self.port = port
        self.user = user
        self.pwd = pwd
        self.sender = sender
        self.receiver_list = receiver_list

        self.smtp = None

    def init(self):
        """ init """
        self.smtp = smtplib.SMTP(timeout=70)
        self.smtp.connect(self.smtp_server, self.port)
        self.smtp.starttls()
        self.smtp.set_debuglevel(0)
    
    def generate_template(self, path):
        kv_list =list((line.strip().split('\t')[0], line.strip().split('\t')[1]) for line in open(path).readlines())
        #kv_list = [(k[0], k[1]) for k in kv_dict.items()]
        d='' #表格内容
        for i in range(len(kv_list)):
            d=d+"""
            <tr>
              <td width="60" align="center">""" + str(kv_list[i][0]) + """</td>
              <td width="60" align="center">""" + str(kv_list[i][1]) + """</td>
            </tr>"""
        
        html = """\
        <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <body>
        <div id="container">
        <p><strong>model train success:</strong></p>
        <div id="content">
         <table width="30%" border="2" bordercolor="black" cellspacing="0" cellpadding="0">
        <tr>
          <td width="60" align="center"><strong>统计指标</strong></td>
          <td width="60" align="center"><strong>统计信息</strong></td>
        </tr>"""+d+"""
        </table>
        </div>
        </div>
        </div>
        </body>
        </html>
              """

        return html

    def send_email(self, subject, msg, is_model_train=False, file_names=[], prefix=''):
        """ send_email """
        msg_root = MIMEMultipart('related')
        msg_root['Subject'] = subject
        msg_root['To'] = COMMASPACE.join(self.receiver_list)
        if is_model_train:
            html = self.generate_template(msg)
        else:
            html = msg
        msg_text = MIMEText('%s' % html, 'html', 'utf-8')
        msg_root.attach(msg_text)

        for file_name in file_names:
            suffix = file_name
            file_name = prefix + file_name
            fp = open(file_name, 'rb')
            file1 = email.mime.base.MIMEBase('application', 'vnd.ms-excel')
            file1.set_payload(fp.read())
            fp.close()
            email.encoders.encode_base64(file1)
            str1 = 'attachment;filename=' + suffix
            file1.add_header('Content-Disposition', str1)
            msg_root.attach(file1)

        while True:
            try:
                self.smtp.login(self.user, self.pwd)
                self.smtp.sendmail(self.sender, self.receiver_list, msg_root.as_string())
                break
            except Exception as e:
                print e
                try:
                    time.sleep(20)
                    self.smtp.connect()
                except Exception as e:
                    logging.error("failed to login to smtp server, e: %s" % str(e))


if __name__ == "__main__":
    subject = sys.argv[1]
    text_body = sys.argv[2]
    model_train = False
    if len(sys.argv) == 4 and sys.argv[3] == 'yes':
        model_train = True
    mailsender = MailSender()
    mailsender.init()
    mailsender.send_email(subject=subject, msg=text_body, is_model_train=model_train)

