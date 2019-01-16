//
// Created by starsnet on 15/5/18.
//
#include "CPALRBidder.h"
#include "soa/jsoncpp/json.h"
#include <math.h>

namespace bayes
{
using namespace std;

CPALRBidder::CPALRBidder(int id)
{
    model_name = "CPALRBidder";
    _cpa_lr_conf_dict_mgr = nullptr;
    model_id = id;
    use_ctr = true;
    use_cvr = true;
}

CPALRBidder::~CPALRBidder()
{
    if (_cpa_lr_conf_dict_mgr != nullptr) {
        delete _cpa_lr_conf_dict_mgr;
    }
    _cpa_lr_conf_dict_mgr = nullptr;
}

int CPALRBidder::init(const Json::Value &parameters)
{
    Json::Value model_dir = parameters["model_dir"];
    Json::Value model_file = parameters["model_file"];

    _cpa_lr_conf_dict_mgr = new CPALRConfDict_mgr_t(model_dir.asCString(),
                                                    model_file.asCString(),
                                                    &CPALRConfDict_mgr_t::content_type::init);
    if (_cpa_lr_conf_dict_mgr == nullptr) {
        return -1;
    }
    if (_cpa_lr_conf_dict_mgr->reload() < 0) {
        return -1;
    }
    return 0;
}

int CPALRBidder::compute_bid(
    std::shared_ptr <BidMax::BidRequest> br,
    const BidMax::AdSpot &spot,
    const BidderCreative &bidCreative,
    bidder_result &result,
    bool is_purchase)
{
    if (result.pctr < 0 || result.pcvr < 0) {
        return -1;
    }

    CPALRConfDict *dict = _cpa_lr_conf_dict_mgr->get_content();

    double compute_ctr = result.pctr;
    if (compute_ctr > dict->max_trust_ctr) {
        compute_ctr = dict->max_trust_ctr;
    }
    double compute_cvr = result.pcvr;
    if (compute_cvr > dict->max_trust_ctr) {
        compute_cvr = dict->max_trust_ctr;
    }

    double ad_cpa = dict->get_ad_cpa_goal(bidCreative);

    result.price = ad_cpa * compute_cvr * dict->cpa_rate;
    result.priority = result.price;
    return 0;
}

int CPALRBidder::model_reloader()
{
    if (_cpa_lr_conf_dict_mgr == nullptr) {
        return -1;
    }
    if (_cpa_lr_conf_dict_mgr->need_reload() == 1) {
        if (_cpa_lr_conf_dict_mgr->reload() < 0) {
            return -1;
        }
    }
    return 0;
}

int CPALRConfDict::init()
{
    return 0;
}

int CPALRConfDict::load(const char *file_dir, const char *file_name)
{
    if (file_dir == nullptr || file_name == nullptr) {
        return -1;
    }
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);

    std::ifstream model_file(filename);
    Json::Value cpa_lr_conf = Json::parse(model_file);

    double max_trust_ctr;
    double max_trust_cvr;
    double cpa_rate;

    Json::Value max_trust_ctr_json = cpa_lr_conf["max_trust_ctr"];
    max_trust_ctr = max_trust_ctr_json.asDouble();
    Json::Value max_trust_cvr_json = cpa_lr_conf["max_trust_cvr"];
    max_trust_cvr = max_trust_cvr_json.asDouble();
    Json::Value cpa_rate_json = cpa_lr_conf["cpa_rate"];
    cpa_rate = cpa_rate_json.asDouble();

    if (cpa_lr_conf.isMember("creatives")) {
        Json::Value creatives_conf = cpa_lr_conf["creatives"];
        Json::Value::iterator iter = creatives_conf.begin();

        while (iter != creatives_conf.end()) {
            const Json::Value &creative_conf = *iter;
            Json::Value creative_id_json = creative_conf["id"];
            int creative_id = creative_id_json.asInt();

            Json::Value creative_goal_json = creative_conf["cpa_goal"];
            double cpa_goal = creative_goal_json.asDouble();
            ad_cpa_goal_map[creative_id] = cpa_goal;
            ++iter;
        }
    }
    return 0;
}

double CPALRConfDict::get_ad_cpa_goal(const BidderCreative &bidCreative)
{
    map<int, double>::const_iterator iter = ad_cpa_goal_map.find(bidCreative.id());
    if (iter != ad_cpa_goal_map.end()) {
        return iter->second;
    } else if (bidCreative.ad_cpa() > 0) {
        return bidCreative.ad_cpa();
    } else {
        return -1;
    }
}
} /* namespace bayes */
