/*
 * CVRMixModelMgr.cpp
 *
 *
 *
 */

#include "CVRMixModelMgr.h"
#include "CVRMixModel.h"

namespace bayes
{
using namespace std;

CVRMixModelMgr::CVRMixModelMgr()
{
    // do nothing
}

CVRMixModelMgr::~CVRMixModelMgr()
{
    for (map<int, CVRMixModel *>::iterator itr = cvr_mix_model_dict.begin();
         itr != cvr_mix_model_dict.end(); ++itr) {
        if (itr->second != nullptr) {
            delete itr->second;
            itr->second = nullptr;
        }
    }
}

int CVRMixModelMgr::init(const Json::Value &parameters)
{
    if (load_model_dict(parameters) < 0) {
        throw ML::Exception("load_model_dict in CVRMixModelMgr error");
        return -1;
    }
    cout << "finish init CVRMixModel mrg" << endl;
    return 0;
}

CVRMixModel *CVRMixModelMgr::get_cvr_mix_model(std::string &model_name)
{
    map<string, int>::const_iterator iter = model_id_dict.find(model_name);
    if (iter == model_id_dict.end()) {
        return nullptr;
    }
    return get_cvr_mix_model(iter->second);
}

CVRMixModel *CVRMixModelMgr::get_cvr_mix_model(int id)
{
    map<int, CVRMixModel *>::const_iterator iter = cvr_mix_model_dict.find(id);
    if (iter == cvr_mix_model_dict.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

int CVRMixModelMgr::get_modelid_list(vector<int> &modelid_list)
{
    for (map<int, CVRMixModel *>::iterator itr = cvr_mix_model_dict.begin();
         itr != cvr_mix_model_dict.end(); ++itr) {
        modelid_list.push_back(itr->first);
    }
    return 0;
}

int CVRMixModelMgr::load_model_dict(const Json::Value &model_json)
{
    Json::Value::const_iterator iter = model_json.begin();

    while (iter != model_json.end()) {
        const Json::Value &model_conf = *iter;
        Json::Value model_name_json = model_conf["model_name"];
        string model_name = model_name_json.asString();
        Json::Value model_id_json = model_conf["model_id"];
        uint32_t model_id = static_cast<uint32_t>(model_id_json.asInt());
        cout << "Load model : " << model_name << " " << model_id << endl;
        if (model_name == "CVRMixModel") {
            bayes::CVRMixModel *model_tmp = new bayes::CVRMixModel(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new cvrmix Model error");
                return -1;
            }

            if (model_tmp->init(model_conf) < 0) {
                cout << "Load model : " << model_name << " " << model_id << endl;
                throw ML::Exception("Load cvrmix Model error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            cvr_mix_model_dict.insert(
                std::pair<int, CVRMixModel *>(model_id, model_tmp));
        } else {
            throw ML::Exception("Load Unkown cvrmix model");
            return -1;
        }
        ++iter;
    }
    cout << "finish load model cvrmix dict" << endl;
    return 0;
}

int CVRMixModelMgr::model_reloader()
{
    for (map<int, CVRMixModel *>::iterator itr = cvr_mix_model_dict.begin();
         itr != cvr_mix_model_dict.end(); ++itr) {
        if (itr->second->model_reloader() < 0) {
            cerr << "Load cvrmix mode error:" << itr->second->get_model_name()
                 << endl;
            return -1;
        }
    }
    return 0;
}
} /* namespace bayes */
