/*
 * bidderbase.cpp
 *
 *  Created on: 2015年3月13日
 *      Author: starsnet
 */

#include "bidderbase.h"

namespace bayes
{
using namespace std;

bidder_base::bidder_base()
{
    model_name = "modelname";
    model_id = 0;
    use_ctr = false;
    use_cvr = false;
    use_load = false;
}

bidder_base::~bidder_base()
{
    // TODO Auto-generated destructor stub
}

int bidder_base::init(const Json::Value &parameters)
{
    return 0;
}

int bidder_base::compute_bid(
    std::shared_ptr <BidMax::BidRequest> br,
    const BidMax::AdSpot &spot,
    const BidderCreative &bidCreative,
    bidder_result &result,
    bool is_purchase)
{
    return 0;
}

int bidder_base::compute_bid_ex(
    std::shared_ptr <BidMax::BidRequest> br,
    const BidMax::AdSpot &spot,
    const BidderCreative &bidCreative,
    bidder_result &result,
    const std::string &flag,
    bool is_purchase)
{
    return 0;
}

int bidder_base::model_reloader()
{
    return 0;
}

const string &bidder_base::get_model_name()
{
    return model_name;
}

bool bidder_base::need_ctr() const
{
    return use_ctr;
}

bool bidder_base::need_cvr() const
{
    return use_cvr;
}

bool bidder_base::need_load() const
{
    return use_load;
}

} /* namespace bayes */
