#include "CPCBidMgr.h"
#include "CPCLRBidder.h"
#include "CPCLinearBidder.h"
#include "CPCConcaveBidder.h"

namespace bayes
{
using namespace std;

CPCBidMgr::CPCBidMgr()
{
    // do nothing
}

CPCBidMgr::~CPCBidMgr()
{
    for (map<int, bidder_base *>::iterator itr = cpc_model_dict.begin();
         itr != cpc_model_dict.end(); ++itr) {
        if (itr->second != nullptr) {
            delete itr->second;
            itr->second = nullptr;
        }
    }
}

int CPCBidMgr::init(const Json::Value &parameters)
{
    if (load_model_dict(parameters) < 0) {
        throw ML::Exception("load_model_dict in CPCBidMgr error");
        return -1;
    }
    return 0;
}

bidder_base *CPCBidMgr::get_bid_model(std::string &model_name)
{
    map<string, int>::const_iterator iter = model_id_dict.find(model_name);
    if (iter == model_id_dict.end()) {
        return nullptr;
    }
    return get_bid_model(iter->second);
}

bidder_base *CPCBidMgr::get_bid_model(int id)
{
    map<int, bidder_base *>::const_iterator iter = cpc_model_dict.find(id);
    if (iter == cpc_model_dict.end()) {
        return nullptr;
    } else {
        return iter->second;
    }
}

int CPCBidMgr::get_modelid_list(vector<int> &modelid_list)
{
    for (map<int, bidder_base *>::iterator itr = cpc_model_dict.begin();
         itr != cpc_model_dict.end(); ++itr) {
        modelid_list.push_back(itr->first);
    }
    return 0;
}

int CPCBidMgr::load_model_dict(const Json::Value &model_json)
{
    Json::Value::const_iterator iter = model_json.begin();

    while (iter != model_json.end()) {
        const Json::Value &model_conf = *iter;
        Json::Value model_name_json = model_conf["model_name"];
        string model_name = model_name_json.asString();
        Json::Value model_id_json = model_conf["model_id"];
        uint32_t model_id = static_cast<uint32_t>(model_id_json.asInt());
        cout << "Load model : " << model_name << " " << model_id << endl;
        if (model_name == "CPCLRBidder") {
            CPCLRBidder *model_tmp = new CPCLRBidder(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new CPCLRBidder error");
                return -1;
            }
            if (model_tmp->init(model_conf) < 0) {
                throw ML::Exception("Load CPCLRBidder error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            cpc_model_dict.insert(
                std::pair<int, bidder_base *>(model_id, model_tmp));
        } else if (model_name == "CPCLinearBidder") {
            CPCLinearBidder *model_tmp = new CPCLinearBidder(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new CPCLinearBidder error");
                return -1;
            }
            if (model_tmp->init(model_conf) < 0) {
                throw ML::Exception("Load CPCLinearBidder error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            cpc_model_dict.insert(
                std::pair<int, bidder_base *>(model_id, model_tmp));
        } else if (model_name == "CPCConcaveBidder") {
            CPCConcaveBidder *model_tmp = new CPCConcaveBidder(model_id);
            if (model_tmp == nullptr) {
                throw ML::Exception("new CPCConcaveBidder error");
                return -1;
            }
            if (model_tmp->init(model_conf) < 0) {
                throw ML::Exception("Load CPCConcaveBidder error");
                delete model_tmp;
                model_tmp = nullptr;
                return -1;
            }
            cpc_model_dict.insert(
                std::pair<int, bidder_base *>(model_id, model_tmp));
        } else {
            throw ML::Exception("Load Unkown model");
            return -1;
        }
        ++iter;
    }
    return 0;
}

int CPCBidMgr::model_reloader()
{
    for (map<int, bidder_base *>::iterator itr = cpc_model_dict.begin();
         itr != cpc_model_dict.end(); ++itr) {
        if (itr->second->model_reloader() < 0) {
            cerr << "ReLoad CPC bid model error:"
                 << itr->second->get_model_name() << endl;
            return -1;
        }
    }
    return 0;
}
} /* namespace bayes */
