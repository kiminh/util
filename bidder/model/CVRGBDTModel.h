#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_CVRGBDTMODEL_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_CVRGBDTMODEL_H_

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

class CVRGBDTModel
    :
        public CVRModelBase
{
public:
    CVRGBDTModel(int id);

    virtual ~CVRGBDTModel();

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
    model_dict::GBDTModel_mgr_t *_gbdt_model_mgr_t;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CVRLRMODEL_H_ */
