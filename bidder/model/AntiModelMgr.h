/*
 * CTRModelMgr.h
 *
 *  Created on: 2015年3月2日
 *      Author: starsnet
 */
#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_ANTIMODELMGR_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_ANTIMODELMGR_H_

#include <string>
#include <map>
#include <vector>
#include "AntiModel.h"

namespace bayes
{
class AntiModelMgr
{
public:
    AntiModelMgr();
    virtual ~AntiModelMgr();
    int load_model_dict(const Json::Value &model_json);
    int init(const Json::Value &parameters);
    AntiModel *get_anti_model(std::string &type);
    AntiModel *get_anti_model(int model_id);
    int get_modelid_list(std::vector<int> &modelid_list);
    int model_reloader();
private:
    std::map<int, AntiModel *> anti_model_dict;
    std::map<std::string, int> model_id_dict;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRMODELMGR_H_ */
