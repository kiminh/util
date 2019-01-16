//
// Created by starsnet on 15/5/12.
//
#include "CVRModelMgr.h"
#include "CVRLRModel.h"
#include "CVRGBDTModel.h"
#include "CVRFMFTRLModel.h"

namespace bayes
{
using namespace std;

CVRModelMgr::CVRModelMgr()
{
    // do nothing
}

CVRModelMgr::~CVRModelMgr()
{
    for (map<int, CVRModelBase *>::iterator itr = cvr_model_dict.begin();
         itr != cvr_model_dict.end(); ++itr) {
        if (itr->second != nullptr) {
            delete itr->second;
            itr->second = nullptr;
        }
    }
}

int CVRModelMgr::init(const Json::Value &parameters)
{
    if (load_model_dict(parameters) < 0) {
        throw ML::Exception("load_model_dict in CVRModelMgr error");
        return -1;
    }
    return 0;
}

CVRModelBase *CVRModelMgr::get_cvr_model(std::string &model_name)
{
    map<string, int>::const_iterator iter = model_id_dict.find(model_name);
    if (iter == model_id_dict.end()) {
        return nullptr;
    }
    return get_cvr_model(iter->second);
}

CVRModelBase *CVRModelMgr::get_cvr_model(int id)
{
    map<int, CVRModelBase *>::const_iterator iter = cvr_model_dict.find(id);
    if (iter == cvr_model_dict.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

int CVRModelMgr::get_modelid_list(vector<int> &modelid_list)
{
    for (map<int, CVRModelBase *>::iterator itr = cvr_model_dict.begin();
         itr != cvr_model_dict.end(); ++itr) {
        modelid_list.push_back(itr->first);
    }
    return 0;
}

int CVRModelMgr::load_model_dict(const Json::Value &model_json)
{
    Json::Value::const_iterator iter = model_json.begin();

    while (iter != model_json.end()) {
        const Json::Value &model_conf = *iter;
        Json::Value model_name_json = model_conf["model_name"];
        string model_name = model_name_json.asString();
        Json::Value model_id_json = model_conf["model_id"];
        uint32_t model_id = static_cast<uint32_t>(model_id_json.asInt());
        cout << "Load model : " << model_name << " " << model_id << endl;
        if (model_name == "CVRLRModel") {
            bayes::CVRLRModel *model_tmp = new bayes::CVRLRModel(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new CVRLRModel error");
                return -1;
            }

            if (model_tmp->init(model_conf) < 0) {
                cout << "Load model : " << model_name << " " << model_id << endl;
                throw ML::Exception("Load CVRLRModel error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            cvr_model_dict.insert(
                std::pair<int, CVRModelBase *>(model_id, model_tmp));
        } else if (model_name == "CVRGBDTModel") {
            bayes::CVRGBDTModel *model_tmp = new bayes::CVRGBDTModel(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new CVRGBDTModel error");
                return -1;
            }

            if (model_tmp->init(model_conf) < 0) {
                cout << "Load model : " << model_name << " " << model_id << endl;
                throw ML::Exception("Load CVRGBDTModel error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            cvr_model_dict.insert(
                std::pair<int, CVRModelBase *>(model_id, model_tmp));
        } else if (model_name == "CVRFMFTRLModel") {
            bayes::CVRFMFTRLModel *model_tmp = new bayes::CVRFMFTRLModel(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new CVRFMFTRLModel error");
                return -1;
            }

            if (model_tmp->init(model_conf) < 0) {
                cout << "Load model : " << model_name << " " << model_id << endl;
                throw ML::Exception("Load CVRFMFTRLModel error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            cvr_model_dict.insert(
                std::pair<int, CVRModelBase *>(model_id, model_tmp));
        }  else {
            throw ML::Exception("Load Unkown CVR model");
            return -1;
        }
        ++iter;
    }
    return 0;
}

int CVRModelMgr::model_reloader()
{
    for (map<int, CVRModelBase *>::iterator itr = cvr_model_dict.begin();
         itr != cvr_model_dict.end(); ++itr) {
        if (itr->second->model_reloader() < 0) {
            cerr << "Load CVR mode error:" << itr->second->get_model_name()
                 << endl;
            return -1;
        }
    }
    return 0;
}
} /* namespace bayes */
