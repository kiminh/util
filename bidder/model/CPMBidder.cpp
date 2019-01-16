#include "CPMBidder.h"
#include "soa/jsoncpp/json.h"
#include <math.h>

namespace bayes
{
using namespace BidMax;
using namespace std;

CPMBidder::CPMBidder(int id)
{
    model_name = "CPMBidder";
    model_id = id;
    use_ctr = true;
    use_cvr = false;
}

CPMBidder::~CPMBidder()
{
    // do nothing
}

int CPMBidder::init(const Json::Value &parameters)
{
    this->bidder_rate = 1.0;
    if (parameters.isMember("bidder_rate")) {
        Json::Value bidder_rate_json = parameters["bidder_rate"];
        bidder_rate = bidder_rate_json.asDouble();
    }

    if (parameters.isMember("ctr_pow")) {
        Json::Value ctr_pow_json = parameters["ctr_pow"];
        this->ctr_pow = ctr_pow_json.asDouble();
        cout << "ctr_pow: " << ctr_pow << endl;
    }
    //[]
    this->max_trust_ctr = 0.8;
    if (parameters.isMember("max_trust_ctr")) {
        Json::Value max_trust_ctr_json = parameters["max_trust_ctr"];
        max_trust_ctr = max_trust_ctr_json.asDouble();
    }
    //cut_ctr
    this->cut_ctr = 0.005;
    if (parameters.isMember("cut_ctr")) {
        Json::Value cut_ctr_json = parameters["cut_ctr"];
        cut_ctr = cut_ctr_json.asDouble();
    }

    this->using_ctr_cut = false;
    if (parameters.isMember("ctr_cut_thr")) {
        Json::Value ctr_cut_json = parameters["ctr_cut_thr"];
        this->ctr_cut_thr = ctr_cut_json.asDouble();
        using_ctr_cut = true;
    }

    return 0;
}

int CPMBidder::compute_bid(
    std::shared_ptr <BidRequest> br,
    const AdSpot &spot,
    const BidderCreative &bidCreative,
    bidder_result &result,
    bool is_purchase)
{
    double base_bid = float(bidCreative.ad_bid());
    double final_ctr = result.pctr;

    //is_purchase开关打开的时候，会ctr有个cut-过滤召回效果
    if (is_purchase) {
        if (result.pctr > max_trust_ctr) {
            final_ctr = max_trust_ctr;
        } else if (result.pctr < cut_ctr) {
            final_ctr = 0.0;
        }
    }

    if (need_ctr()) {
        result.priority = final_ctr * result.price;
    } else {
        result.priority = 1 * result.price;
    }
    result.price = base_bid / 1000;
    result.priority = result.price * final_ctr;

    return 0;
}

int CPMBidder::model_reloader()
{
    return 0;
}
} /* namespace bayes */
