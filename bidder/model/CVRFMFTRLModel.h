#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_CVRFMFTRLMODEL_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_CVRFMFTRLMODEL_H_

#include "bidmax/common/bids.h"
#include "bidmax/common/bid_request.h"
#include "CVRModelBase.h"
#include "soa/service/dual_reloader.h"
#include "ModelDict.h"
#include <unordered_map>
#include <set>

namespace bayes
{

class CVRFMFTRLModel
    :
        public CVRModelBase
{
public:
    CVRFMFTRLModel(int id);

    virtual ~CVRFMFTRLModel();

    virtual int init(const Json::Value &parameters);

    virtual int model_reloader();

    virtual double compute_cvr(
        std::shared_ptr <BidRequest> br,
        AdSpot &spot,
        const BidderCreative &bidCreative,
        const Json::Value &augmentation,
        int threadId,
        std::vector<uint32_t> & feas);

private:
    model_dict::FMFtrlModel_mgr_t *_fmftrl_mgr_t;

    bool using_gbdt;
    model_dict::GBDTModel_mgr_t *_gbdt_model_mgr_t;
    uint32_t gbdt_hash_bias;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CVRFMFTRLMODEL_H_ */
