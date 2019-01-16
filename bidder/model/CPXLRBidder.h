/*
 * CPXLRBidder.h
 *
 *  Created on: 2015年3月14日
 *      Author: starsnet
 */

#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_CPXLRBIDDER_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_CPXLRBIDDER_H_

#include "bidderbase.h"
#include "bidmax/common/bid_request.h"
#include "soa/service/dual_reloader.h"
#include <vector>
#include <map>
#include "bidmax/common/str_util.h"

namespace bayes
{

struct AdCVRDict
{
    int init();

    int load(const char *file_dir, const char *file_name);

    double get_cvr(int aduser);

    std::unordered_map<int, double> ad_cvr_dict;
    double target_cpa;
};

typedef DICT::reloader_t <AdCVRDict> AdCVRDict_mgr_t;

class CPXLRBidder : public bidder_base
{
public:
    CPXLRBidder(int id);

    virtual ~CPXLRBidder();

    virtual int init(const Json::Value &parameters);

    virtual int model_reloader();

    virtual int compute_bid(
        std::shared_ptr <BidMax::BidRequest> br,
        const BidMax::AdSpot &spot,
        const BidderCreative &bidCreative,
        bidder_result &result,
        bool is_purchase = false);

    virtual int compute_bid_ex(
        std::shared_ptr <BidMax::BidRequest> br,
        const BidMax::AdSpot &spot,
        const BidderCreative &bidCreative,
        bidder_result &result,
        const std::string &flag,
        bool is_purchase);

    int get_targets(int, double &, double &);
private:
    double max_trust_ctr;
    double cut_ctr;
    double bidder_rate;
    double cvr_odd_ratio;

    AdCVRDict_mgr_t *_ad_cvr_dict_mgr_t;
    double dft_target_cvr;

    bool use_target_cpa;
    bool using_target_cpa;
    double dft_target_cpa;
    double target_cvr_high;
    double target_cvr_low;

    int explore_threshold;
    int explore_base;
    double explore_rate;

};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CPXLRBIDDER_H_ */
