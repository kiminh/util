/*
 * AntiModelMgr.cpp
 *
 *  Created on: 2015年3月2日
 *      Author: starsnet
 */

#include "AntiModelMgr.h"
#include "AntiModel.h"

namespace bayes
{
using namespace std;

AntiModelMgr::AntiModelMgr()
{
    // do nothing
}

AntiModelMgr::~AntiModelMgr()
{
    for (map<int, AntiModel *>::iterator itr = anti_model_dict.begin();
         itr != anti_model_dict.end(); ++itr) {
        if (itr->second != nullptr) {
            delete itr->second;
            itr->second = nullptr;
        }
    }
}

int AntiModelMgr::init(const Json::Value &parameters)
{
    if (load_model_dict(parameters) < 0) {
        throw ML::Exception("load_model_dict in AntiModelMgr error");
        return -1;
    }
    cout << "finish init antimodel mrg" << endl;
    return 0;
}

AntiModel *AntiModelMgr::get_anti_model(std::string &model_name)
{
    map<string, int>::const_iterator iter = model_id_dict.find(model_name);
    if (iter == model_id_dict.end()) {
        return nullptr;
    }
    return get_anti_model(iter->second);
}

AntiModel *AntiModelMgr::get_anti_model(int id)
{
    map<int, AntiModel *>::const_iterator iter = anti_model_dict.find(id);
    if (iter == anti_model_dict.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

int AntiModelMgr::get_modelid_list(vector<int> &modelid_list)
{
    for (map<int, AntiModel *>::iterator itr = anti_model_dict.begin();
         itr != anti_model_dict.end(); ++itr) {
        modelid_list.push_back(itr->first);
    }
    return 0;
}

int AntiModelMgr::load_model_dict(const Json::Value &model_json)
{
    Json::Value::const_iterator iter = model_json.begin();

    while (iter != model_json.end()) {
        const Json::Value &model_conf = *iter;
        Json::Value model_name_json = model_conf["model_name"];
        string model_name = model_name_json.asString();
        Json::Value model_id_json = model_conf["model_id"];
        uint32_t model_id = static_cast<uint32_t>(model_id_json.asInt());
        cout << "Load model : " << model_name << " " << model_id << endl;
        if (model_name == "AntiModel") {
            bayes::AntiModel *model_tmp = new bayes::AntiModel(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new anti Model error");
                return -1;
            }

            if (model_tmp->init(model_conf) < 0) {
                cout << "Load model : " << model_name << " " << model_id << endl;
                throw ML::Exception("Load anti Model error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            anti_model_dict.insert(
                std::pair<int, AntiModel *>(model_id, model_tmp));
        } else {
            throw ML::Exception("Load Unkown anti model");
            return -1;
        }
        ++iter;
    }
    cout << "finish load model anti dict" << endl;
    return 0;
}

int AntiModelMgr::model_reloader()
{
    for (map<int, AntiModel *>::iterator itr = anti_model_dict.begin();
         itr != anti_model_dict.end(); ++itr) {
        if (itr->second->model_reloader() < 0) {
            cerr << "Load anti mode error:" << itr->second->get_model_name()
                 << endl;
            return -1;
        }
    }
    return 0;
}
} /* namespace bayes */
