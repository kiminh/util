//
// Created by starsnet on 15/5/18.
//

#ifndef BAYES_RTBKIT_CPALRBIDDER_H
#define BAYES_RTBKIT_CPALRBIDDER_H

#include "bidderbase.h"
#include "soa/service/dual_reloader.h"
#include "bidmax/common/bid_request.h"
#include <vector>
#include <map>

namespace bayes
{
struct CPALRConfDict
{
    int init();
    int load(const char *file_dir, const char *file_name);
    double get_ad_cpa_goal(const BidderCreative &bidCreative);
    double max_trust_ctr;
    double max_trust_cvr;
    double cpa_rate;
    std::map<int, double> ad_cpa_goal_map;
};

typedef DICT::reloader_t <CPALRConfDict> CPALRConfDict_mgr_t;

class CPALRBidder : public bidder_base
{
public:
    CPALRBidder(int id);
    virtual ~CPALRBidder();
    virtual int init(const Json::Value &parameters);
    virtual int model_reloader();
    virtual int compute_bid(
        std::shared_ptr <BidMax::BidRequest> br,
        const BidMax::AdSpot &spot,
        const BidderCreative &bidCreative,
        bidder_result &result,
        bool is_purchase = false);
private:
    CPALRConfDict_mgr_t *_cpa_lr_conf_dict_mgr;
};

} /* namespace bayes */

#endif //BAYES_RTBKIT_CPALRBIDDER_H
