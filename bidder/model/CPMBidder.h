#ifndef BIDMAX_CPMBIDDER_H
#define BIDMAX_CPMBIDDER_H

#include "bidderbase.h"
#include "soa/service/dual_reloader.h"
#include "bidmax/common/bid_request.h"
#include "bidmax/common/str_util.h"
#include <vector>
#include <map>
#include <set>

namespace bayes
{

class CPMBidder : public bidder_base
{
public:
    CPMBidder(int id);
    virtual ~CPMBidder();
    virtual int init(const Json::Value &parameters);
    virtual int model_reloader();
    virtual int compute_bid(
        std::shared_ptr <BidMax::BidRequest> br,
        const BidMax::AdSpot &spot,
        const BidderCreative &bidCreative,
        bidder_result &result,
        bool is_purchase = false);

private:
    double ctr_pow;
    bool using_ctr_cut;
    double ctr_cut_thr;
    double max_trust_ctr;
    bool using_load_cut;
    double load_cut_thr;
    double cut_ctr;
};

} /* namespace bayes */

#endif //BIDMAX_CPMBIDDER_H
