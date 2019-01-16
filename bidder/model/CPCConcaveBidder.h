#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCCONCAVEBIDDER_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCCONCAVEBIDDER_H_

#include "bidderbase.h"
#include "soa/service/dual_reloader.h"
#include "bidmax/common/bid_request.h"
#include "bidmax/common/str_util.h"
#include <vector>
#include <map>
#include <set>
#include <utility>
#include <math.h>

namespace bayes
{
struct BidDict
{
    int init();
    int load(const char *file_dir, const char *file_name);
    int get_paras(int aduser, double &, double &);
    int get_clk(int, uint32_t &);
    std::unordered_map<int, std::pair<double, double> > aduser_bid_dict;
    std::unordered_map<int, uint32_t> aduser_clk_dict;
};

typedef DICT::reloader_t <BidDict> BidDict_mgr_t;

class CPCConcaveBidder : public bidder_base
{
public:
    CPCConcaveBidder(int id);
    virtual ~CPCConcaveBidder();
    virtual int init(const Json::Value &parameters);
    virtual int model_reloader();
    virtual int compute_bid(
        std::shared_ptr <BidMax::BidRequest> br,
        const BidMax::AdSpot &spot,
        const BidderCreative &bidCreative,
        bidder_result &result,
        bool is_purchase);

private:
    double calc_bid(double lambda, double c, double pvalue, int lambda_scale);

private:
    BidDict_mgr_t *_aduser_bid_mgr_t;
    int lambda_scale;
    int default_aduser;

    uint32_t clk_thr;

    double tracking_bidder_rate;
    double notracking_bidder_rate;
    //uint32_t cvr_show_thr;
    //double bidder_rate;
    //double ctr_pow;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCCONCAVEBIDDER_H_ */
