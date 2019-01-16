#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRLRKVMODEL_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRLRKVMODEL_H_

#include "bidmax/common/bids.h"
#include "bidmax/common/bid_request.h"
#include "CTRModelBase.h"
#include "soa/service/dual_reloader.h"
#include "ModelDict.h"
#include <unordered_map>
#include <set>
#include "soa/jsoncpp/json.h"
#include "jml/utils/file_functions.h"
#include "gbdt.h"

namespace bayes
{

class CTRLRKVModel
    :
        public CTRModelBase
{
public:
    CTRLRKVModel(int id);

    virtual ~CTRLRKVModel();

    virtual int init(const Json::Value &parameters);

    virtual int model_reloader();

    virtual double compute_ctr(
        std::shared_ptr <BidRequest> br,
        AdSpot &spot,
        const BidderCreative &bidCreative,
        const Json::Value &augmentation,
        int threadId,
        std::vector<uint32_t> & feas);

private:
    model_dict::FtrlKVModel_mgr_t *_ftrl_kv_model_mgr_t;

    bool using_gbdt;
    model_dict::GBDTModel_mgr_t *_gbdt_model_mgr_t;
    uint32_t gbdt_hash_bias;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRLRMODEL_H_ */
