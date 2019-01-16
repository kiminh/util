#ifndef BAYES_RTBKIT_CVRMIXMODEL_H
#define BAYES_RTBKIT_CVRMIXMODEL_H

#include "bidmax/common/bids.h"
#include "bidmax/common/bid_request.h"
#include "CVRModelBase.h"
#include "soa/service/dual_reloader.h"
#include  <unordered_map>
#include "ModelDict.h"
#include "soa/jsoncpp/json.h"
#include <utility>
#include "CTRLRModel.h"

namespace bayes
{

class CVRMixModel
    :
        public CVRModelBase
{
public:
    CVRMixModel(int id);

    virtual ~CVRMixModel();

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
    model_dict::FtrlModel_mgr_t *_cvr_model_mgr_t;

    double belta_bias;

    model_dict::FMFtrlModel_mgr_t *_fmftrl_mgr_t;
    bool using_fm;
    double fm_bias;

    std::vector <fea::extractor> _fea_extractors;
    std::vector <fea::instance> _instances;

    const static int threadNum = 4;
};

} /* namespace bayes */

#endif //BAYES_RTBKIT_CVRMixModel_H
