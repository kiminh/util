#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCLRBIDDER_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCLRBIDDER_H_

#include "bidderbase.h"
#include "bidmax/common/bid_request.h"
#include "soa/service/dual_reloader.h"
#include <vector>
#include <map>

namespace bayes
{
struct CPCLRConfDict
{
    int init();
    int load(const char *file_dir, const char *file_name);
    double get_cpc_goal(const BidderCreative &bidCreative);
    double ctr_pow;
    double rate;
    double max_trust_ctr;
    double default_cpc_goal;
    std::map<int, double> cpc_goal_map;
};

typedef DICT::reloader_t <CPCLRConfDict> CPCLRConfDict_mgr_t;

class CPCLRBidder : public bidder_base
{
public:
    CPCLRBidder(int id);
    virtual ~CPCLRBidder();
    virtual int init(const Json::Value &parameters);
    virtual int model_reloader();
    virtual int compute_bid(
        std::shared_ptr <BidMax::BidRequest> br,
        const BidMax::AdSpot &spot,
        const BidderCreative &bidCreative,
        bidder_result &result,
        bool is_purchase = false);
private:
    CPCLRConfDict_mgr_t *_cpc_lr_conf_dict_mgr_t;
};

} /* namespace bayes */
#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCLRBIDDER_H_ */
