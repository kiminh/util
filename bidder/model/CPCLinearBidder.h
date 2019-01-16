/*
 * CPCLinearBidder.h
 *
 *  Created on: 2015年3月18日
 *      Author: starsnet
 */

#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCLINEARBIDDER_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCLINEARBIDDER_H_

#include "bidderbase.h"
#include "soa/service/dual_reloader.h"
#include "bidmax/common/bid_request.h"
#include "bidmax/common/str_util.h"
#include <vector>
#include <map>
#include <set>
#include <unordered_set>

namespace bayes
{
class CPCLinearBidder : public bidder_base
{
public:
    CPCLinearBidder(int id);

    virtual ~CPCLinearBidder();

    virtual int init(const Json::Value &parameters);

    virtual int model_reloader();

    virtual int compute_bid(
        std::shared_ptr <BidMax::BidRequest> br,
        const BidMax::AdSpot &spot,
        const BidderCreative &bidCreative,
        bidder_result &result,
        bool is_purchase);

private:
    uint32_t cvr_show_thr;
    double bidder_rate;
    double cvr_bidder_rate;

    double q_T;

    bool using_ctr_cut;
    double ctr_cut_thr;
    bool using_cvr_cut;
    double cvr_cut_thr;
    double load_thr;
    bool using_random;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCLINEARBIDDER_H_ */
