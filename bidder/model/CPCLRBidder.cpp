/*
 * CPCLRBidder.cpp
 *
 *  Created on: 2015年3月14日
 *      Author: starsnet
 */

#include "CPCLRBidder.h"
#include "soa/jsoncpp/json.h"
#include <math.h>

namespace bayes
{
using namespace BidMax;
using namespace std;

CPCLRBidder::CPCLRBidder(int id)
{
    model_name = "CPCLRBidder";
    _cpc_lr_conf_dict_mgr_t = nullptr;
    model_id = id;
    use_ctr = true;
}

CPCLRBidder::~CPCLRBidder()
{
    if (_cpc_lr_conf_dict_mgr_t != nullptr) {
        delete _cpc_lr_conf_dict_mgr_t;
    }
    _cpc_lr_conf_dict_mgr_t = nullptr;
}

int CPCLRBidder::init(const Json::Value &parameters)
{
    Json::Value model_dir = parameters["model_dir"];
    Json::Value model_file = parameters["model_file"];

    _cpc_lr_conf_dict_mgr_t = new CPCLRConfDict_mgr_t(model_dir.asCString(),
                                                      model_file.asCString(),
                                                      &CPCLRConfDict_mgr_t::content_type::init);
    if (_cpc_lr_conf_dict_mgr_t == nullptr) {
        return -1;
    }
    if (_cpc_lr_conf_dict_mgr_t->reload() < 0) {
        return -1;
    }
    return 0;
}

int CPCLRBidder::compute_bid(
    std::shared_ptr <BidRequest> br,
    const AdSpot &spot,
    const BidderCreative &bidCreative,
    bidder_result &result,
    bool is_purchase)
{
    if (!need_ctr() || result.pctr < 0) {
        return -1;
    }

    CPCLRConfDict *dict = _cpc_lr_conf_dict_mgr_t->get_content();

    if (result.pctr > dict->max_trust_ctr) {
        result.pctr = dict->max_trust_ctr;
    }

    double cpc_goal = dict->get_cpc_goal(bidCreative);

    result.price = cpc_goal * pow(result.pctr, dict->ctr_pow) * dict->rate;
    result.priority = result.price;
    return 0;
}

int CPCLRBidder::model_reloader()
{
    if (_cpc_lr_conf_dict_mgr_t == nullptr) {
        return -1;
    }
    if (_cpc_lr_conf_dict_mgr_t->need_reload() == 1) {
        if (_cpc_lr_conf_dict_mgr_t->reload() < 0) {
            return -1;
        }
    }
    return 0;
}

int CPCLRConfDict::init()
{
    this->default_cpc_goal = 0.5;
    return 0;
}

int CPCLRConfDict::load(const char *file_dir, const char *file_name)
{
    if (file_dir == nullptr || file_name == nullptr) {
        return -1;
    }
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);

    std::ifstream model_file(filename);
    Json::Value cpc_lr_conf = Json::parse(model_file);

    Json::Value max_trust_ctr_json = cpc_lr_conf["max_trust_ctr"];
    max_trust_ctr = max_trust_ctr_json.asDouble();
    Json::Value bidder_rate_json = cpc_lr_conf["bidder_rate"];
    rate = bidder_rate_json.asDouble();
    Json::Value ctr_pow_json = cpc_lr_conf["ctr_pow"];
    ctr_pow = ctr_pow_json.asDouble();
    Json::Value default_cpc_goal_json = cpc_lr_conf["default_cpc_goal"];
    default_cpc_goal = default_cpc_goal_json.asDouble();

    if (cpc_lr_conf.isMember("creatives")) {
        Json::Value creatives_conf = cpc_lr_conf["creatives"];
        Json::Value::iterator iter = creatives_conf.begin();

        while (iter != creatives_conf.end()) {
            const Json::Value &creative_conf = *iter;
            Json::Value creative_id_json = creative_conf["id"];
            int creative_id = creative_id_json.asInt();

            Json::Value creative_goal_json = creative_conf["cpc_goal"];
            double cpc_goal = creative_goal_json.asDouble();
            cpc_goal_map[creative_id] = cpc_goal;
            ++iter;
        }
    }
    return 0;
}

double CPCLRConfDict::get_cpc_goal(const BidderCreative &bidCreative)
{
    map<int, double>::const_iterator iter = cpc_goal_map.find(bidCreative.id());
    if (iter != cpc_goal_map.end()) {
        return iter->second;
    } else if (bidCreative.ad_bid() > 0) {
        return bidCreative.ad_bid();
    } else {
        return default_cpc_goal;
    }
}

} /* namespace bayes */
