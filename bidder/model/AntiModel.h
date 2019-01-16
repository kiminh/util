#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_ANTIMODEL_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_ANTIMODEL_H_

#include <string>
#include "bidmax/common/bids.h"
#include "bidmax/common/bid_request.h"
#include "CTRModelBase.h"
#include "soa/service/dual_reloader.h"
#include "bidmax/common/str_util.h"
#include <unordered_set>
#define APP_TAG 1
#define USER_TAG 2
#define NEW_TAG 3

namespace bayes
{

struct AntiDict
{
    int init();
    int load(const char *file_dir, const char *file_name);
    bool is_medium_spam(std::string);
    bool is_user_spam(std::string, int);
    bool is_user_new(std::string);
    std::unordered_set <std::string> spam_app_set;
    std::unordered_set <std::string> spam_user_set;
    std::unordered_set <std::string> old_user_set;
};

typedef DICT::reloader_t <AntiDict> AntiDict_mgr_t;

class AntiModel
{
public:
    AntiModel(int id);
    virtual ~AntiModel();
    virtual int init(const Json::Value &parameters);
    virtual int model_reloader();
    bool is_medium_spam(std::string);
    bool is_user_spam(std::string, int);
    bool is_user_new(std::string);

    const std::string &get_model_name()
    {
        return model_name;
    }

private:
    AntiDict_mgr_t *_anti_mgr_t;
    std::string model_name;
    int model_id;
    bool enable;

    bool enable_newuser_filter;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRLRMODEL_H_ */
