//
// Created by starsnet on 15/5/12.
//

#ifndef BAYES_RTBKIT_CVRMODELMGR_H
#define BAYES_RTBKIT_CVRMODELMGR_H

#include <string>
#include <map>
#include <vector>

#include "CVRModelBase.h"

namespace bayes
{
class CVRModelMgr
{
public:
    CVRModelMgr();

    virtual ~CVRModelMgr();

    int load_model_dict(const Json::Value &model_json);

    int init(const Json::Value &parameters);

    CVRModelBase *get_cvr_model(std::string &type);

    CVRModelBase *get_cvr_model(int model_id);

    int get_modelid_list(std::vector<int> &modelid_list);

    int model_reloader();

private:
    std::map<int, CVRModelBase *> cvr_model_dict;
    std::map<std::string, int> model_id_dict;
};

} /* namespace bayes */

#endif //BAYES_RTBKIT_CVRMODELMGR_H
