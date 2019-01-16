//
// Created by starsnet on 16/3/11.
//

#ifndef BIDMAX_CPMBIDMGR_H
#define BIDMAX_CPMBIDMGR_H

#include <string>
#include <map>
#include <vector>
#include "bidderbase.h"

namespace bayes
{
class CPMBidMgr
{
public:
    CPMBidMgr();
    virtual ~CPMBidMgr();
    int load_model_dict(const Json::Value &model_json);
    int init(const Json::Value &parameters);
    bidder_base *get_bid_model(std::string &type);
    bidder_base *get_bid_model(int model_id);
    int get_modelid_list(std::vector<int> &modelid_list);
    int model_reloader();
private:
    std::map<int, bidder_base *> cpm_model_dict;
    std::map<std::string, int> model_id_dict;
};
} /* namespace bayes */

#endif //BIDMAX_CPMBIDMGR_H
