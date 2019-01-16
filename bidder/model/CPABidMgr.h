//
// Created by starsnet on 15/5/12.
//

#ifndef BAYES_RTBKIT_CPABIDMGR_H
#define BAYES_RTBKIT_CPABIDMGR_H

#include <string>
#include <map>
#include <vector>
#include "bidderbase.h"

namespace bayes
{
class CPABidMgr
{
public:
    CPABidMgr();

    virtual ~CPABidMgr();

    int load_model_dict(const Json::Value &model_json);

    int init(const Json::Value &parameters);

    bidder_base *get_bid_model(std::string &type);

    bidder_base *get_bid_model(int model_id);

    int get_modelid_list(std::vector<int> &modelid_list);

    int model_reloader();

private:
    std::map<int, bidder_base *> cpa_model_dict;
    std::map<std::string, int> model_id_dict;
};
} /* namespace bayes */


#endif //BAYES_RTBKIT_CPABIDMGR_H
