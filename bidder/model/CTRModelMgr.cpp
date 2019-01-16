/*
 * CTRModelMgr.cpp
 *
 *  Created on: 2015年3月2日
 *      Author: starsnet
 */

#include "CTRModelMgr.h"
#include "CTRLRModel.h"
#include "CTRFMFTRLModel.h"
#include "CTRLRKVModel.h"

namespace bayes
{
using namespace std;

CTRModelMgr::CTRModelMgr()
{
    // do nothing
}

CTRModelMgr::~CTRModelMgr()
{
    for (map<int, CTRModelBase *>::iterator itr = ctr_model_dict.begin();
         itr != ctr_model_dict.end(); ++itr) {
        if (itr->second != nullptr) {
            delete itr->second;
            itr->second = nullptr;
        }
    }
}

int CTRModelMgr::init(const Json::Value &parameters)
{
    if (load_model_dict(parameters) < 0) {
        throw ML::Exception("load_model_dict in CTRModelMgr error");
    }
    return 0;
}

CTRModelBase *CTRModelMgr::get_ctr_model(std::string &model_name)
{
    map<string, int>::const_iterator iter = model_id_dict.find(model_name);
    if (iter == model_id_dict.end()) {
        return nullptr;
    }
    return get_ctr_model(iter->second);
}

CTRModelBase *CTRModelMgr::get_ctr_model(int id)
{
    map<int, CTRModelBase *>::const_iterator iter = ctr_model_dict.find(id);
    if (iter == ctr_model_dict.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

int CTRModelMgr::get_modelid_list(vector<int> &modelid_list)
{
    for (map<int, CTRModelBase *>::iterator itr = ctr_model_dict.begin();
         itr != ctr_model_dict.end(); ++itr) {
        modelid_list.push_back(itr->first);
    }
    return 0;
}

int CTRModelMgr::load_model_dict(const Json::Value &model_json)
{
    Json::Value::const_iterator iter = model_json.begin();

    while (iter != model_json.end()) {
        const Json::Value &model_conf = *iter;
        Json::Value model_name_json = model_conf["model_name"];
        string model_name = model_name_json.asString();
        Json::Value model_id_json = model_conf["model_id"];
        uint32_t model_id = static_cast<uint32_t>(model_id_json.asInt());
        cout << "Load model : " << model_name << " " << model_id << endl;
        if (model_name == "CTRLRModel") {
            bayes::CTRLRModel *model_tmp = new bayes::CTRLRModel(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new CTRLRModel error");
            }

            if (model_tmp->init(model_conf) < 0) {
                cout << "Load model : " << model_name << " " << model_id << endl;
                throw ML::Exception("Load CTRLRModel error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            ctr_model_dict.insert(
                std::pair<int, CTRModelBase *>(model_id, model_tmp));
        }else if (model_name == "CTRLRKVModel") {
            bayes::CTRLRKVModel *model_tmp = new bayes::CTRLRKVModel(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new CTRLRKVModel error");
            }

            if (model_tmp->init(model_conf) < 0) {
                cout << "Load model : " << model_name << " " << model_id << endl;
                throw ML::Exception("Load CTRLRKVModel error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            ctr_model_dict.insert(
                std::pair<int, CTRModelBase *>(model_id, model_tmp));
        }else if (model_name == "CTRFMFTRLModel") {
            bayes::CTRFMFTRLModel *model_tmp = new bayes::CTRFMFTRLModel(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new CTRFMFTRLModel error");
            }

            if (model_tmp->init(model_conf) < 0) {
                cout << "Load model : " << model_name << " " << model_id << endl;
                throw ML::Exception("Load CTRFMFTRLModel error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            ctr_model_dict.insert(
                std::pair<int, CTRModelBase *>(model_id, model_tmp));

        } else {
            throw ML::Exception("Load Unkown CTR model");
            return -1;
        }
        ++iter;
    }
    return 0;
}

int CTRModelMgr::model_reloader()
{
    for (map<int, CTRModelBase *>::iterator itr = ctr_model_dict.begin();
         itr != ctr_model_dict.end(); ++itr) {
        if (itr->second->model_reloader() < 0) {
            cerr << "Load CTR mode error:" << itr->second->get_model_name()
                 << endl;
            return -1;
        }
    }
    return 0;
}
} /* namespace bayes */
