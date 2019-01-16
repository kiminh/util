/*************************************************************************
    > File Name: extractor.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 五 12/19 23:08:08 2014
 ************************************************************************/
#include <map>
#include <fstream>
#include <iostream>
#include "conf_util.h"
#include "fea_result.h"
#include "extractor.h"
#include "beta.h"
#include "combined_fea.h"
#include "direct_fea.h"
#include "location_fea.h"
#include "device_id_fea.h"
#include "ip_fea.h"
#include "domain_fea.h"
#include "installed_num_fea.h"

namespace fea
{
using namespace std;
bool extractor::init(const char *conf)
{
    util::conf_util extractor_conf;
    extractor_conf.parse(conf);
    string pattern;
    cout << "Begin init schema... " << endl;
    if (extractor_conf.has_item("pattern")) {
        pattern = extractor_conf.getItem<string>("pattern");
        m_record.parse_schema(pattern);
    } else {
        cout << "Schema not set!" << endl;
        return false;
    }

    string fea_conf;
    if (extractor_conf.has_item("fea_conf")) {
        fea_conf = extractor_conf.getItem<string>("fea_conf");
    } else {
        cout << "fea_conf error!" << endl;
        return false;
    }

    cout << "Begin init feaList..." << endl;

    if (!init_fea_list(fea_conf, pattern)) {
        cout << "Fea list init error.." << endl;
        return false;
    }

    if (extractor_conf.has_item("enable_hash")) {
        cout << "Adfea enable hash..." << endl;
        enable_hash = extractor_conf.getItem<bool>("enable_hash");
    } else {
        cout << "Adfea Not enable hash..." << endl;
        enable_hash = false;
    }

    if (extractor_conf.has_item("hash_num")) {
        hash_num = extractor_conf.getItem<int>("hash_num");
    } else {
        hash_num = 0;
    }

    cout << "Begin init result..." << endl;
    init_result();

    //检查和设置特征提取的模式
    if (extractor_conf.has_item("label_index")) {
        string label_string = extractor_conf.getItem<string>("label_index");
        if (m_record.contain_field("label_index")) {
            label_index = m_record.m_pattern[label_string];
            if (label_index != atoi(label_string.c_str())) {
                cout << "label_index set error" << endl;
                return false;
            }
        } else {
            cout << "Schema error, not contain label_index" << endl;
            return false;
        }
        cout << "Offline adfea. label inedx= " << label_index << endl;
        on_line = false;
    } else {
        on_line = true;
    }
    return true;
}

void extractor::init_result()
{
    result.enable_hash = this->enable_hash;
    result.hash_num = this->hash_num;

    for (size_t index = 0; index < vec_fea_list.size(); ++index) {
        fea_t fea_item;
        fea_item.is_output = vec_fea_list[index]->is_output_fea();
        result.put_fea(vec_fea_list[index]->get_name(), fea_item);
    }
}

bool extractor::init_fea_list(string &fea_conf, string &pattern)
{
    m_record.parse_schema(pattern);
    ifstream fea_stream(fea_conf);
    string line;
    vector <string> vec_fea;

    cout << "begin parse fea_list" << endl;
    while (getline(fea_stream, line)) {
        util::str_util::trim(line);
        if (line.empty() || line[0] == '#') {
            continue;
        }

        fea_arg m_fea_arg;
        m_fea_arg.parse_fea_arg(line);
        fea_base *fea_item = get_fea_method(m_fea_arg.method);
        if (fea_item == NULL) {
            cout << "init lr_fea item " << m_fea_arg.fea_name << " error" << endl;
            clear();
            return false;
        }
        fea_item->set_fea_arg(m_fea_arg);
        fea_item->check_arg(m_record);
        fea_item->init();
        vec_fea_list.push_back(fea_item);
    }
    return true;
}

fea_base *extractor::get_fea_method(std::string &fea_name)
{
    if (fea_name == "beta") {
        return new beta();
    } else if (fea_name == "combined_fea") {
        return new combined_fea();
    } else if (fea_name == "direct_fea") {
        return new direct_fea();
    } else if (fea_name == "device_id_fea") {
        return new device_id_fea();
    } else if (fea_name == "location_fea") {
        return new location_fea();
    } else if (fea_name == "ip_fea") {
        return new ip_fea();
    } else if (fea_name == "domain_fea") {
        return new domain_fea();
    } else if (fea_name == "installed_num_fea") {
        return new installed_num_fea();
    } else {
        return nullptr;
    }
}

bool extractor::add_record(map <string, string> &field_map)
{
    return m_record.add_record(field_map);
}

bool extractor::add_record(vector <string> &fields)
{
    return m_record.add_record(fields);
}

bool extractor::extract_fea()
{
    result.clear();
    vector<fea_base *>::iterator iter = vec_fea_list.begin();

    while (iter != vec_fea_list.end()) {
        (*iter)->extract_fea(m_record, result);
        ++iter;
    }
    return true;
}

void extractor::record_reset()
{
    m_record.clear();
    result.clear();
}

void extractor::get_fea_result(instance &lr_result)
{
    result.to_fea_list(lr_result.fea_vec);
    if (!on_line) {
        lr_result.label = atoi(m_record.valueAt(label_index).c_str());
    }
}

void extractor::get_fea_list(fea::fea_list &list)
{
    result.to_fea_list(list);
}

void extractor::clear()
{
    vector<fea_base *>::iterator iter = vec_fea_list.begin();
    while (iter != vec_fea_list.end()) {
        delete *iter;
        *iter = NULL;
        ++iter;
    }
    vec_fea_list.clear();
}
}
