#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCBIDMGR_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCBIDMGR_H_

#include <string>
#include <map>
#include <vector>
#include "bidderbase.h"

namespace bayes
{
class CPCBidMgr
{
public:
    CPCBidMgr();
    virtual ~CPCBidMgr();
    int load_model_dict(const Json::Value &model_json);
    int init(const Json::Value &parameters);
    bidder_base *get_bid_model(std::string &type);
    bidder_base *get_bid_model(int model_id);
    int get_modelid_list(std::vector<int> &modelid_list);
    int model_reloader();

private:
    std::map<int, bidder_base *> cpc_model_dict;
    std::map<std::string, int> model_id_dict;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CPCBIDMGR_H_ */
