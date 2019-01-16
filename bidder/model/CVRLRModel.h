#ifndef BAYES_RTBKIT_CVRLRMODEL_H
#define BAYES_RTBKIT_CVRLRMODEL_H

#include "bidmax/common/bids.h"
#include "bidmax/common/bid_request.h"
#include "CVRModelBase.h"
#include "soa/service/dual_reloader.h"
#include "ModelDict.h"
#include <unordered_map>
#include <set>
#include "soa/jsoncpp/json.h"
#include "jml/utils/file_functions.h"
#include "gbdt.h"

namespace bayes
{

class CVRLRModel
    :
        public CVRModelBase
{
public:
    CVRLRModel(int id);

    virtual ~CVRLRModel();

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
    model_dict::FtrlModel_mgr_t *_ftrl_model_mgr_t;

    bool using_gbdt;
    model_dict::GBDTModel_mgr_t *_gbdt_model_mgr_t;
    uint32_t gbdt_hash_bias;
};

} /* namespace bayes */

#endif //BAYES_RTBKIT_CVRLRMODEL_H
