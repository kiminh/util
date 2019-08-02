#!/usr/bin/env python

import urllib2
import json
import sys

class DingDingAPI(object):
    def post_json(self, url, body):
        # alternative way
        #opener = urllib2.build_opener()
        #req = urllib2.Request(url, data=body,
        #                      headers={'Content-Type': 'application/json'})
        #response = opener.open(req)

        req = urllib2.Request(url, body, {'Content-Type': 'application/json'});
        response = urllib2.urlopen(req);
        the_page = response.read();
        ret_obj = json.loads(the_page);

        return ret_obj;

    def robot_notify(self, msg):
	url = "https://oapi.dingtalk.com/robot/send?access_token=62c4651627ae00d026395d9193816d96ee8e4cd9f71868445e6e269d89186dc7"
        content = {
            "msgtype": "text",
            "text": {
                "content": msg 
            }
        }

        body = json.dumps(content);

        return self.req_with_retry(self.post_json, (url, body));

    def req_with_retry(self, func, args):
        # only when access_token expires should we retry; you can see from the code that for other cases we simply return
        for x in range(0, 2):
            ret_obj = func(*args); 

            if type(ret_obj) is dict and "errmsg" in ret_obj:
                if ret_obj["errmsg"] == 'ok':
                    print "command done successfully"
                    return (True, ret_obj); 
                elif "errcode" in ret_obj and ret_obj["errcode"] == 40014:
                    if self.reconnect() == False:
                        return (False, ret_obj);
                    else:
                        continue;
            else    :
                print "unable to execute the command";
                print ret_obj;
                return (False, ret_obj);

if __name__ == '__main__':
    # test
    dingding = DingDingAPI();
    arg_number = len(sys.argv)
    if (arg_number < 2):
        print("need at least 2 params for this")
        exit(0)
    message = ""
    i = 1
    while i < len(sys.argv):
        message = message + " " + sys.argv[i]
        i = i + 1
    dingding.robot_notify(message)
