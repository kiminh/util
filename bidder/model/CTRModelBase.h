#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRMODELBASE_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRMODELBASE_H_

#include <string>
#include "soa/jsoncpp/json.h"
#include "bidmax/common/bid_request.h"
#include "bidmax/common/bids.h"
#include "ModelDict.h"
#include "fea/extractor.h"
#include <algorithm>
#include "bidmax/common/bidder_creative.pb.h"

namespace bayes
{

typedef BidderCreatives_BidderCreative BidderCreative;

using namespace BidMax;

class CTRModelBase
{
public:
    CTRModelBase();

    CTRModelBase(string model_name, int id);

    virtual ~CTRModelBase();

    virtual int init(const Json::Value &parameters) = 0;

    virtual int model_reloader();

    virtual double compute_ctr(
        std::shared_ptr <BidRequest> br,
        AdSpot &spot,
        const BidderCreative &bidCreative,
        const Json::Value &augmentation,
        int threadId,
        std::vector<uint32_t> & feas) = 0;

    const std::string &get_model_name();

    int get_post_showclk(const std::string, uint32_t &, uint32_t &);

    //根据according br_fields ad_fields.compute!
    virtual int extract_br(std::shared_ptr <BidRequest> br, AdSpot &spot, int threadId);

protected:
    int make_fields(
        std::shared_ptr <BidRequest> br,
        AdSpot &spot,
        const BidderCreative &bidCreative,
        std::vector <std::string> &fields);

    int make_br_fields(
        std::shared_ptr <BidRequest> br,
        AdSpot &spot,
        std::vector <std::string> &fields);

    int make_ad_fields(
        const BidderCreative &bidCreative,
        const Json::Value &augmentation,
        std::vector <std::string> &fields);

    std::string model_name;
    int model_id;

    double belta_bias;

    vector <fea::extractor> _fea_extractors;
    vector <fea::extractor> _fea_br_extractors;
    vector <fea::extractor> _fea_ad_extractors;

    vector <fea::instance> _br_instances;
    vector <fea::instance> _ad_instances;
    vector <fea::instance> _instances;

    vector <vector<string>> _br_fieldses;
    vector <vector<string>> _ad_fieldses;

    const static int threadNum = 4;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRMODELBASE_H_ */
