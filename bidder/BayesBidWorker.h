#ifndef RTBKIT_BAYES_IMP_BAYES_BIDDER_BAYESBIDWORKER_H_
#define RTBKIT_BAYES_IMP_BAYES_BIDDER_BAYESBIDWORKER_H_

#include "jml/utils/file_functions.h"
#include "soa/service/dual_reloader.h"
#include "soa/jsoncpp/json.h"
#include "model/CTRModelMgr.h"
#include "model/CVRModelMgr.h"
#include "model/CVRModelBase.h"
#include "model/CPABidMgr.h"
#include "model/CPCBidMgr.h"
#include "model/CPMBidMgr.h"
#include "model/bidderbase.h"
#include "model/CTRModelBase.h"
#include "model/CVRMixModel.h"
#include "model/CVRMixModelMgr.h"
#include <string>
#include <random>
#include "model/CPXBidMgr.h"
#include "bidmax/common/bidder_creative.pb.h"

typedef BidderCreatives_BidderCreative BidderCreative;

namespace bayes
{

using namespace BidMax;

struct ExchangeCTRDict
{
    int init();
    int load(const char *file_dir, const char *file_name);
    double get_ctr(std::string);
    std::unordered_map<std::string, double> ctr_map;
};

typedef DICT::reloader_t <ExchangeCTRDict> ExchangeCTRDict_mgr_t;

static inline Json::Value loadJsonFromFile(const std::string &filename)
{
    ML::File_Read_Buffer buf(filename);
    return Json::parse(std::string(buf.start(), buf.end()));
}

struct model_info_t
{
    int cpm_model_id;
    int cpc_model_id;
    int cpa_model_id;
    int ctr_model_id;
    int cvr_model_id;
    int cvr_mix_model_id;
    int medium_model_id;
    int load_model_id;
    int h5_model_id;
    int app_model_id;
    int cpx_model_id;
    int wap_ctr_model_id;

    model_info_t()
    {
        cpm_model_id = NOT_VALID_ID;
        cpc_model_id = NOT_VALID_ID;
        cpa_model_id = NOT_VALID_ID;
        ctr_model_id = NOT_VALID_ID;
        cvr_model_id = NOT_VALID_ID;
        cvr_mix_model_id = NOT_VALID_ID;
        load_model_id = NOT_VALID_ID;
        h5_model_id = NOT_VALID_ID;
        app_model_id = NOT_VALID_ID;
        cpx_model_id = NOT_VALID_ID;
        wap_ctr_model_id = NOT_VALID_ID;
    }

    const static int NOT_VALID_ID = -1;
};

struct AB_config
{
    const static int MOD = 100;

    int init();

    int load(const char *file_dir, const char *file_name);

    int load_model_setting(int *model_config, const Json::Value &model_json);

    int select_model(std::shared_ptr <BidRequest> br, model_info_t &model_info);

    int cpm_model[MOD];
    int cpc_model[MOD];
    int cpa_model[MOD];
    int ctr_model[MOD];
    int cvr_model[MOD];
    int cvr_mix_model[MOD];
    int load_model[MOD];
    int h5_model[MOD];
    int app_model[MOD];
    int cpx_model[MOD];
    int wap_ctr_model[MOD];

    bool enable_ctr;
    bool enable_cvr;
    bool enable_cpm;
    bool enable_cpa;
    bool enable_cpc;
    bool enable_cvr_mix;
    bool enable_load;
    bool enable_h5;
    bool enable_app;
    bool enable_cpx;
    bool enable_wap_ctr;

    std::hash <std::string> str_hash;
    std::default_random_engine generator;
    std::uniform_int_distribution<int> *dis;
};

typedef DICT::reloader_t <AB_config> AB_config_mgr_t;

struct ConstantBiddingController {
public:
    ConstantBiddingController()
    {
        // do nothing
    };

    int init();

    int load(const char * file_dir, const char * file_name);

    double bidRatio(
            int current_minute,
            int start_minute,
            int end_minute,
            int count,
            int total_count);

private:
    std::vector<double> average_bidding_rate;

    int show_threshold;

    double ratio_threshold;

    double boost_ratio;

    size_t length;
};

typedef DICT::reloader_t<ConstantBiddingController> ConstantBiddingControllerLoader;

class Monitor
{
public:
    Monitor()
    {
        running = false;
        reload_time = 10;

        _ctr_model_mgr = nullptr;
        _cvr_model_mgr = nullptr;
        _cpm_bid_mgr = nullptr;
        _cpc_bid_mgr = nullptr;
        _cpa_bid_mgr = nullptr;
        _ab_config_mgr_t = nullptr;
        _load_model_mgr = nullptr;
        _h5_model_mgr = nullptr;
        _app_model_mgr = nullptr;
        _cpx_bid_mgr = nullptr;
        _wap_ctr_model_mgr = nullptr;
        _constant_bidding_controller_loader = nullptr;
    }

    int init(
            int reload_time_in,
            CTRModelMgr *_ctr_model_mgr,
            CVRModelMgr *_cvr_model_mgr,
            CPMBidMgr *_cpm_bid_mgr,
            CPCBidMgr *_cpc_bid_mgr,
            CPABidMgr *_cpa_bid_mgr,
            CVRMixModelMgr *_load_model_mgr,
            CTRModelMgr *_h5_model_mgr,
            CTRModelMgr *_app_model_mgr,
            CPXBidMgr *_cpx_bid_mgr,
            CTRModelMgr *_wap_ctr_model_mgr);

    void add_AB(AB_config_mgr_t *_ab_config_mgr_t);

    void addConstantBiddingController(
            ConstantBiddingControllerLoader * constant_bidding_controller_loader);

    ~Monitor();

    int run();

    void join();

    void stop();

    void callback();

    static void *_thread(void *);

private:
    bool running;
    int reload_time;
    pthread_t thread;

    CTRModelMgr *_ctr_model_mgr;
    CVRModelMgr *_cvr_model_mgr;
    CPMBidMgr *_cpm_bid_mgr;
    CPABidMgr *_cpa_bid_mgr;
    CPCBidMgr *_cpc_bid_mgr;
    AB_config_mgr_t *_ab_config_mgr_t;

    CVRMixModelMgr *_load_model_mgr;

    CTRModelMgr *_h5_model_mgr;

    CTRModelMgr *_app_model_mgr;

    CPXBidMgr *_cpx_bid_mgr;

    CTRModelMgr *_wap_ctr_model_mgr;

    ConstantBiddingControllerLoader * _constant_bidding_controller_loader;

};

class BayesBidWorker
{
public:
    BayesBidWorker()
    {
        for (int i = 0; i < threadNum; ++i) {
            _ctr_models.push_back(nullptr);
            _cvr_models.push_back(nullptr);
            _cpm_bidder_models.push_back(nullptr);
            _cpc_bidder_models.push_back(nullptr);
            _cpa_bidder_models.push_back(nullptr);
            _cpx_bidder_models.push_back(nullptr);
            _load_models.push_back(nullptr);
            _h5_models.push_back(nullptr);
            _app_models.push_back(nullptr);
            _wap_ctr_models.push_back(nullptr);
        }

        _ab_config_mgr_t = nullptr;
        _constant_bidding_controller_loader = nullptr;

    }

    virtual ~BayesBidWorker()
    {
        // do nothing
    }

    int init(const Json::Value &parameters);

    int do_bidder_purchase(
            std::shared_ptr <BidRequest> br,
            AdSpot &spot,
            const BidderCreative &bidCreative,
            std::string &account_id,
            bidder_result &result,
            const Json::Value &augmentation,
            bool is_purchase,
            int threadId);

    int set_model_info(model_info_t &model_info, int threadId);

    int select_model(std::shared_ptr <BidRequest> br, model_info_t &model_info);

    int isBid(const BidderCreative & bidCreative, float pctr);

    bool filter_dc(std::shared_ptr <BidMax::BidRequest> br);

    int init_br_info(const std::shared_ptr <BidRequest> &br, AdSpot &spot, int threadId);

    double get_exchange_ctr(const std::shared_ptr <BidRequest> &br);

private:
    double bidRatioForDaily(int count, int total_count);

    double bidRatioForTotal(int count, int total_count, int start_time, int end_time);

public:
    std::function<void (std::string msg)> onWarningLog;

private:
    vector<CTRModelBase *> _ctr_models;
    vector<CVRModelBase *> _cvr_models;
    vector<bidder_base *> _cpm_bidder_models;
    vector<bidder_base *> _cpc_bidder_models;
    vector<bidder_base *> _cpa_bidder_models;
    vector<bidder_base *> _cpx_bidder_models;

    vector<CVRMixModel *> _load_models;

    vector<CTRModelBase *> _h5_models;
    vector<CTRModelBase *> _app_models;

    vector<CTRModelBase *> _wap_ctr_models;

    CTRModelMgr _ctr_model_mgr;
    CTRModelMgr _wap_ctr_model_mgr;
    CVRModelMgr _cvr_model_mgr;
    CPMBidMgr _cpm_bid_mgr;
    CPCBidMgr _cpc_bid_mgr;
    CPABidMgr _cpa_bid_mgr;

    CPXBidMgr _cpx_bid_mgr;

    AB_config_mgr_t *_ab_config_mgr_t;

    ConstantBiddingControllerLoader * _constant_bidding_controller_loader;

    CVRMixModelMgr _load_model_mgr;

    CTRModelMgr _h5_model_mgr;

    CTRModelMgr _app_model_mgr;

    Monitor monitor;

    bool using_load_filter;
    double load_thr;

    double purchase_thr;

    double real_ctr_ratio;

    ExchangeCTRDict_mgr_t *_exchange_ctr_dict_mgr_t;
    double dft_exchange_ctr;
    bool using_exchange_cpc;

    int show_threshold;

    static const int threadNum = 4;

};

} /* namespace bayes */

#endif /* RTBKIT_BAYES_IMP_BAYES_BIDDER_BAYESBIDWORKER_H_ */
