#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_SHOWMODEL_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_SHOWMODEL_H_

#include <string>
#include "bidmax/common/bids.h"
#include "bidmax/common/bid_request.h"
#include "bidmax/common/str_util.h"
#include "CTRModelBase.h"
#include "soa/service/dual_reloader.h"
#include <unordered_map>

namespace bayes
{

struct ShowDict
{
    int init();

    int load(const char *file_dir, const char *file_name);

    int extract_showclk(std::string &key, std::string &fea_res);

    //std::map<std::string,int> show_dict;
    //std::map<std::string,int> clk_dict;
    //std::map<std::string,std::string> showclk_dict;
    std::unordered_map <std::string, std::string> showclk_dict;
    //int _get_info(std::map<std::string,int>,std::string);

};

typedef DICT::reloader_t <ShowDict> ShowDict_mgr_t;

class ShowModel
{
public:
    ShowModel(int id);

    virtual ~ShowModel();

    virtual int init(const Json::Value &parameters);

    virtual int model_reloader();

    virtual int compute_fea(
        std::shared_ptr <BidRequest> br,
        AdSpot &spot,
        const BidderCreative &bidCreative,
        std::string &account,
        std::string &result);

    const std::string &get_model_name()
    {
        return model_name;
    }

private:
    int make_fields(
        std::shared_ptr <BidRequest> br,
        AdSpot &spot,
        const BidderCreative &bidCreative,
        std::string &account,
        std::string &key);

private:
    ShowDict_mgr_t *_show_mgr_t;
    int max_show;
    std::string model_name;
    int model_id;
    bool enable;
};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_CTRLRMODEL_H_ */
