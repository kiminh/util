import guldan
class GuldanReposity(object):
    __metaclass__ = SingletonEx
    """
    get configuration data from guldan
    """

    EnumGuldanHostPrefix = {
        "eu": "guldan://eu.guldan.corp.cootek.com",
        "us": "guldan://uscasv2.guldan.corp.cootek.com",
        "ap": "guldan://ap.guldan.corp.cootek.com",
        "cn": "guldan://zh.guldan.corp.cootek.com",
    }

    def __init__(self, region_name):
        try:
            self.guldan_client = guldan.Client.instance(auto_refresh=True)
        except Exception as e:
            logging.error("Guldan initializes error.")
        self.region_name = region_name
        self.guldan_uri = ""
        self.result = {}

    def getJsonContent(self, uri):
        self.result = self.getContent(uri)
        if self.result:
            self.result = json.loads(self.result)
        return self.result

    def getContent(self, uri):
        try:
            uri = "%s/%s" % (GuldanReposity.EnumGuldanHostPrefix.get(self.region_name), uri)
            self.guldan_uri = guldan.uri_parse(uri)
            guldan_dict = guldan.rid_with_dict(self.guldan_uri)
            self.guldan_client.subscribe(guldan_dict, self.callback)
            resource = self.guldan_client.get_resource_by_info(self.guldan_uri)
            guldan_json = json.loads(str(resource))
            state_code = guldan_json.get('code', 0)
            if state_code == 200:
                self.result = guldan_json.get('content')
                logging.debug("content is %s" % self.result)
            else:
                logging.debug("State code is %s" % state_code)
        except Exception as e:
            logging.error("Get content from guldan error. ErrMsg: %s" % e, exc_info=True)
        return self.result

    def callback(self, rid, result):
        try:
            logging.debug("Guldan update %s, result is %s" % (rid, result))
            guldan_json = json.loads(str(result))
            self.result = guldan_json.get('content')
        except Exception as e:
            logging.error("Guldan update error, %s" % e)
