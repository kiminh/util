/*
 * CPCLinearBidder.cpp
 *
 *  Created on: 2015年3月18日
 *      Author: starsnet
 */
#include "CPCLinearBidder.h"
#include "soa/jsoncpp/json.h"
#include <math.h>
#include <iostream>
namespace bayes
{
using namespace std;

CPCLinearBidder::CPCLinearBidder(int id)
{
    model_name = "CPCLinearBidder";
    model_id = id;
    use_ctr = true;
    use_cvr = false;
    use_load = false;
}

CPCLinearBidder::~CPCLinearBidder()
{
    // do nothing
}

int CPCLinearBidder::init(const Json::Value &parameters)
{
    if (parameters.isMember("use_cvr")) {
        Json::Value use_cvr_json = parameters["use_cvr"];
        this->use_cvr = use_cvr_json.asBool();
        if (use_cvr) {
            Json::Value cvr_bidder_rate_json = parameters["cvr_bidder_rate"];
            cvr_bidder_rate = cvr_bidder_rate_json.asDouble();
            cout << "cvr_bidder_rate : " << cvr_bidder_rate << endl;
        }
    }
    cout << "use cvr : " << this->use_cvr << endl;

    if (parameters.isMember("use_load")) {
        Json::Value use_load_json = parameters["use_load"];
        this->use_load = use_load_json.asBool();
    }
    if (use_load) {
        Json::Value load_thr_json = parameters["load_thr"];
        load_thr = load_thr_json.asDouble();
    }
    cout << "need load : " << this->use_load << endl;

    Json::Value bidder_rate_json = parameters["bidder_rate"];
    this->bidder_rate = bidder_rate_json.asDouble();
    cout << "bidder_rate : " << bidder_rate << endl;

    this->q_T = 1;
    if (parameters.isMember("q_T")) {
        Json::Value q_T_json = parameters["q_T"];
        this->q_T = q_T_json.asBool();
    }
    cout << "q_T: " << q_T << endl;

    this->using_ctr_cut = false;
    if (parameters.isMember("using_ctr_cut")) {
        Json::Value using_json = parameters["using_ctr_cut"];
        this->using_ctr_cut = using_json.asBool();
    }
    if (this->using_ctr_cut) {
        Json::Value ctr_cut_json = parameters["ctr_cut_thr"];
        this->ctr_cut_thr = ctr_cut_json.asDouble();
    }

    this->using_cvr_cut = false;
    if (parameters.isMember("using_cvr_cut")) {
        Json::Value using_json = parameters["using_cvr_cut"];
        this->using_cvr_cut = using_json.asBool();
    }
    if (this->using_cvr_cut) {
        Json::Value cvr_cut_json = parameters["cvr_cut_thr"];
        this->cvr_cut_thr = cvr_cut_json.asDouble();
    }

    this->using_random = false;
    if (parameters.isMember("using_random")) {
        Json::Value using_json = parameters["using_random"];
        this->using_random = using_json.asBool();
    }

    return 0;
}

int CPCLinearBidder::compute_bid(
    std::shared_ptr <BidMax::BidRequest> br,
    const BidMax::AdSpot &spot,
    const BidderCreative &bidCreative,
    bidder_result &result,
    bool is_purchase)
{
    double base_bid = bidCreative.ad_bid();
    double ctr_goal = bidCreative.target_ctr();
    double cvr_goal = bidCreative.target_cpa();

    double final_pctr = result.pctr;
    if (using_ctr_cut && result.pctr > ctr_cut_thr) {
        final_pctr = ctr_cut_thr;
    }
    double final_pcvr = result.pcvr;
    if (using_cvr_cut && result.pcvr > cvr_cut_thr) {
        final_pcvr = cvr_cut_thr;
    }

    if (bidCreative.tracking_type() == BidMax::Tracking_Type::BID_MAX && use_load) {
        if (result.pload < this->load_thr) {
            cout << "filter h5 for low pload: " << result.pload << endl;
            result.price = 0;
            result.priority = 0;
        }
    }

    double odd = pow(final_pctr / ctr_goal, q_T);
    result.price = odd * base_bid * bidder_rate;

    if (use_cvr && final_pcvr > 0.0 &&
        bidCreative.tracking_type() != BidMax::Tracking_Type::NOTRACKING &&
        cvr_goal > 0.0) {
        double cvr_odd = pow(final_pcvr / cvr_goal, q_T);
        result.price = cvr_odd * base_bid * cvr_bidder_rate;
        result.priority = result.price * final_pctr;
    } else {
        result.priority = result.price * final_pctr;
    }

    if (using_random) {
        result.price = base_bid;
        result.priority = result.price;
    }

    return 0;
}

int CPCLinearBidder::model_reloader()
{
    return 0;
}

} /* namespace bayes */
