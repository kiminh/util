/*
 * CTRModelMgr.h
 *
 *  Created on: 2015年3月2日
 *      Author: starsnet
 */

#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_MEDIUMMODELMGR_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_MEDIUMMODELMGR_H_

#include <string>
#include <map>
#include <vector>

#include "MediumModel.h"

namespace bayes
{
class MediumModelMgr
{
public:
    MediumModelMgr();
    virtual ~MediumModelMgr();
    int load_model_dict(const Json::Value &model_json);
    int init(const Json::Value &parameters);
    MediumModel *get_medium_model(std::string &type);
    MediumModel *get_medium_model(int model_id);
    int get_modelid_list(std::vector<int> &modelid_list);
    int model_reloader();
private:
    std::map<int, MediumModel *> medium_model_dict;
    std::map<std::string, int> model_id_dict;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRMODELMGR_H_ */
