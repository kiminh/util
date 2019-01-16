#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_BIDDERBASE_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_BIDDERBASE_H_

#include "soa/jsoncpp/json.h"
#include "bidmax/common/bidder_result.h"
#include "bidmax/common/bid_request.h"
#include "bidmax/common/bidder_creative.pb.h"
#include "bidmax/common/agent_config.h"
#include <string>

namespace bayes
{

typedef BidderCreatives_BidderCreative BidderCreative;

class bidder_base
{
public:
    bidder_base();
    virtual ~bidder_base();
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
    const std::string &get_model_name();
    virtual bool need_ctr() const;
    virtual bool need_cvr() const;
    virtual bool need_load() const;
protected:
    std::string model_name;
    bool use_ctr;
    bool use_cvr;
    bool use_load;
    int model_id;
    double bidder_rate;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_BIDDERBASE_H_ */
