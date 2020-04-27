#coding:utf-8
import sys
import requests
import json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger('flow')

# ====================== #
GULDAN_USER_HASH = '4b746c7be6dafd619a39a24565b682d9'
GULDAN_UPGRADE_URL = '{base_url}/api/item/{resource_id}/upgrade'
GULDAN_API_MAP = {
    'cn': 'http://zh.guldan.corp.cootek.com',
    'us': 'http://uscasv2.guldan.corp.cootek.com',
    'ap': 'http://ap.guldan.corp.cootek.com',
    'eu': 'http://eu.guldan.corp.cootek.com',
}
GULDAN_RESOURCE_SEARCH_URL = '{base_url}/api/resource/search?q={resource_name}'
GULDAN_GET_RESOURCE_URL = '{base_url}/api/item/{resource_id}'
GULDAN_RESOURCE_NAME = 'naga_dsp_interactive.page_select_module.middle_page' #'naga_dsp_interactive.page_select_module.for_test'
# ====================== #


class Guldan(object):

    def __init__(self):
        self.headers = {
            'X-Guldan-Token': GULDAN_USER_HASH,
        }
        self.region = 'cn'
        self.base_url = GULDAN_API_MAP[self.region]
        self.resource_id = self.get_resource_id()

    def get_resource_id(self):
        res = requests.get(GULDAN_RESOURCE_SEARCH_URL.format(base_url=self.base_url, resource_name=GULDAN_RESOURCE_NAME),
                           headers=self.headers)
        res_json = json.loads(res.text)
        resource_id = res_json.get('data')[0].get('id')
        return resource_id

    def get_current_resource_content(self):
        res = requests.get(GULDAN_GET_RESOURCE_URL.format(base_url=self.base_url, resource_id=self.resource_id),
                           headers=self.headers)
        res_json = json.loads(res.text)
        content = res_json.get('data').get('content')
        return json.loads(content)

    def get_icon_list(self):
        content_json = self.get_current_resource_content()
        icon_list = content_json['icon_list']
        return icon_list
     
    def del_icon_list(self, del_icon_list):
        content_json = self.get_current_resource_content()
        icon_list = content_json['icon_list']
        icon_set = set(icon_list)
        trigger_del_icon = []
        for icon in del_icon_list:
            if icon in icon_set:
                icon_set.remove(icon)
                trigger_del_icon.append(icon)

        content_json['icon_list'] = list(icon_set)
        upgrade_body = {
            "content": json.dumps(content_json, ensure_ascii=False, indent=4),
            "type": "json",
            "private": "false"
        }
        res = requests.post(GULDAN_UPGRADE_URL.format(base_url=self.base_url, resource_id=self.resource_id),
                data=json.dumps(upgrade_body),
                headers=self.headers)
        return trigger_del_icon, res.status_code
     
    def get_stream_image_list(self):
        content_json = self.get_current_resource_content()
        feeds_list = content_json['feeds_list']
        feeds_vertical_list = content_json['feeds_vertical_list']
        feeds_image_list = []
        for pageid in feeds_list:
            for item in feeds_list[pageid]:
                feeds_image_list.append(item['image'])
        
        for pageid in feeds_vertical_list:
            for item in feeds_list[pageid]:
                feeds_image_list.append(item['image'])

        return feeds_image_list
     
    def del_stream_image_list(self, del_image_list):
        content_json = self.get_current_resource_content()
        image_list = self.get_stream_image_list()
        feeds_list = content_json['feeds_list']
        feeds_vertical_list = content_json['feeds_vertical_list']


        trigger_del_image = []
        for image in del_image_list:
            if image in image_list:
                trigger_del_image.append(image)

        new_feeds_list = {}
        new_feeds_vertical_list = {}

        for pageid in feeds_list:
            for item in feeds_list[pageid]:
                if item['image'] in trigger_del_image:
                    continue
                if pageid not in new_feeds_list:
                    new_feeds_list[pageid] = []
                new_feeds_list[pageid].append(item)
    
        for pageid in feeds_vertical_list:
            for item in feeds_vertical_list[pageid]:
                if item['image'] in trigger_del_image:
                    continue
                if pageid not in new_feeds_vertical_list:
                    new_feeds_vertical_list[pageid] = []
                new_feeds_vertical_list[pageid].append(item)
         
        content_json['feeds_list'] = new_feeds_list
        content_json['feeds_vertical_list'] = new_feeds_vertical_list

        upgrade_body = {
            "content": json.dumps(content_json, ensure_ascii=False, indent=4),
            "type": "json",
            "private": "false"
        }
        res = requests.post(GULDAN_UPGRADE_URL.format(base_url=self.base_url, resource_id=self.resource_id),
                data=json.dumps(upgrade_body),
                headers=self.headers)
        return trigger_del_image, res.status_code
        
if __name__ == '__main__':
    guldan = Guldan()
    del_icon = [
        "http://cdn.convergemob.com/AD/AD_INTERACTIVE/image/2020012004.jpg"
    ]
    #result_msg = guldan.get_stream_image_list()

    result_msg, _ = guldan.del_stream_image_list(del_icon)
    print(result_msg), type(result_msg)
