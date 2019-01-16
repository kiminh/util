/*
 * CTRModelWorker.cpp
 *
 *  Created on: 2015年3月1日
 *      Author: starsnet
 */
#include "BayesBidWorker.h"
#include "bidmax/common/agent_config.h"

namespace bayes
{
using std::pair;
using namespace std;
using namespace BidMax;

int BayesBidWorker::select_model(
        std::shared_ptr <BidRequest> br,
        model_info_t &model_info)
{
    AB_config *config = _ab_config_mgr_t->get_content();
    if (config == nullptr) {
        throw ML::Exception("AB_config in select_model null");
    }

    if (config->select_model(br, model_info) < 0) {
        return -1;
    }
    return 0;
}

double BayesBidWorker::bidRatioForDaily(int count, int total_count)
{
    if (total_count <= 0) {
        return 1;
    }

    if (count >= total_count) {
        return 0;
    }

    auto * controller = _constant_bidding_controller_loader->get_content();
    auto now = Date::now();
    // time zone
    now.addHours(8);
    int c_minute = ((now.hour() + 24) % 24) * 60 + now.minute();
    double bid_ratio = controller->bidRatio(
            c_minute,
            0,
            1440,
            count,
            total_count);

    return bid_ratio;
}

// start_time 0:0:0
// end_time 24:0:0
double BayesBidWorker::bidRatioForTotal(int count, int total_count, int start_time, int end_time)
{
    if (start_time == -1 || end_time == -1 || total_count <= 0) {
        return 1;
    }

    if (count >= total_count) {
        return 0;
    }

    auto * controller = _constant_bidding_controller_loader->get_content();
    auto now = Date::now();

    auto latest_date = Date::fromSecondsSinceEpoch(start_time - start_time % 86400).addHours(-8);
    auto start_date = Date::fromSecondsSinceEpoch(start_time);
    auto end_date = Date::fromSecondsSinceEpoch(end_time);

    int c_minute = static_cast<int>(now.secondsSince(latest_date)) / 60;
    int s_minite = static_cast<int>(start_date.secondsSince(latest_date)) / 60;
    int e_minute = static_cast<int>(end_date.secondsSince(latest_date)) / 60;

    double bid_ratio = controller->bidRatio(
            c_minute,
            s_minite,
            e_minute,
            count,
            total_count);

    return bid_ratio;
}

int BayesBidWorker::isBid(const BidderCreative & bidCreative, float pctr)
{
    if (bidCreative.is_constant_bidding() == 0) {
        return 1;
    }

    if (bidCreative.ad_show_daily() < show_threshold) {
        return 1;
    }

    float real_ctr = static_cast<float>(bidCreative.ad_click_daily()) / bidCreative.ad_show_daily();
    if (pctr > real_ctr * real_ctr_ratio) {
        return 1;
    }

    double rand_ratio = static_cast<double>(rand()) / RAND_MAX;

    double click_daily_ratio = bidRatioForDaily(
            bidCreative.ad_click_daily(),
            bidCreative.ad_clicks_limit_daily());

    if (rand_ratio > click_daily_ratio) {
        return 0;
    }

    double show_daily_ratio = bidRatioForDaily(
            bidCreative.ad_show_daily(),
            bidCreative.ad_shows_limit_daily());

    if (rand_ratio > show_daily_ratio) {
        return 0;
    }

    double budget_daily_ratio = bidRatioForDaily(
            bidCreative.ad_consume_daily(),
            bidCreative.ad_budget_daily_limit());

    if (rand_ratio > budget_daily_ratio) {
        return 0;
    }

    double click_total_ratio = bidRatioForTotal(
            bidCreative.ad_click_total(),
            bidCreative.ad_clicks_limit_total(),
            bidCreative.start_time(),
            bidCreative.end_time());

    if (rand_ratio > click_total_ratio) {
        return 0;
    }

    double show_total_ratio = bidRatioForTotal(
            bidCreative.ad_show_total(),
            bidCreative.ad_shows_limit_total(),
            bidCreative.start_time(),
            bidCreative.end_time());

    if (rand_ratio > show_total_ratio) {
        return 0;
    }

    return 1;
}

int BayesBidWorker::set_model_info(model_info_t &model_info, int threadId)
{
    auto &_ctr_model = _ctr_models[threadId];
    auto &_cvr_model = _cvr_models[threadId];
    auto &_cpm_bidder_model = _cpm_bidder_models[threadId];
    auto &_cpc_bidder_model = _cpc_bidder_models[threadId];
    auto &_cpa_bidder_model = _cpa_bidder_models[threadId];
    auto &_load_model = _load_models[threadId];
    auto &_h5_model = _h5_models[threadId];
    auto &_app_model = _app_models[threadId];
    auto &_cpx_bidder_model = _cpx_bidder_models[threadId];

    auto &_wap_ctr_model = _wap_ctr_models[threadId];

    if (model_info.ctr_model_id != model_info_t::NOT_VALID_ID) {
        _ctr_model = _ctr_model_mgr.get_ctr_model(model_info.ctr_model_id);
        if (_ctr_model == nullptr) {
            cout << "get ctr model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    if (model_info.cvr_model_id != model_info_t::NOT_VALID_ID) {
        _cvr_model = _cvr_model_mgr.get_cvr_model(model_info.cvr_model_id);
        if (_cvr_model == nullptr) {
            cout << "get cvr model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    if (model_info.cpm_model_id != model_info_t::NOT_VALID_ID) {
        _cpm_bidder_model = _cpm_bid_mgr.get_bid_model(model_info.cpm_model_id);
        if (_cpm_bidder_model == nullptr) {
            cout << "get _cpm_bidder_model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    if (model_info.cpc_model_id != model_info_t::NOT_VALID_ID) {
        _cpc_bidder_model = _cpc_bid_mgr.get_bid_model(model_info.cpc_model_id);
        if (_cpc_bidder_model == nullptr) {
            cout << "get _cpc_bidder_model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    if (model_info.cpa_model_id != model_info_t::NOT_VALID_ID) {
        _cpa_bidder_model = _cpa_bid_mgr.get_bid_model(model_info.cpa_model_id);
        if (_cpa_bidder_model == nullptr) {
            cout << "get _cpa_bidder_model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    if (model_info.load_model_id != model_info_t::NOT_VALID_ID) {
        _load_model = _load_model_mgr.get_cvr_mix_model(model_info.load_model_id);
        if (_load_model == nullptr) {
            cout << "get _load_model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    if (model_info.h5_model_id != model_info_t::NOT_VALID_ID) {
        _h5_model = _h5_model_mgr.get_ctr_model(model_info.h5_model_id);
        if (_h5_model == nullptr) {
            cout << "get _h5_model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    if (model_info.app_model_id != model_info_t::NOT_VALID_ID) {
        _app_model = _app_model_mgr.get_ctr_model(model_info.app_model_id);
        if (_app_model == nullptr) {
            cout << "get _app_model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    if (model_info.cpx_model_id != model_info_t::NOT_VALID_ID) {
        _cpx_bidder_model = _cpx_bid_mgr.get_bid_model(model_info.cpx_model_id);
        if (_cpx_bidder_model == nullptr) {
            cout << "get _cpx_model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    if (model_info.wap_ctr_model_id != model_info_t::NOT_VALID_ID) {
        _wap_ctr_model = _wap_ctr_model_mgr.get_ctr_model(model_info.wap_ctr_model_id);
        if (_wap_ctr_model == nullptr) {
            cout << "get ctr model error in BayesBidWorker" << endl;
            return -1;
        }
    }

    return 0;
}

int BayesBidWorker::init_br_info(const std::shared_ptr <BidRequest> &br, AdSpot &spot, int threadId)
{
    auto &_ctr_model = _ctr_models[threadId];
    auto &_wap_ctr_model = _wap_ctr_models[threadId];

    if (br->app) {
        if (_ctr_model->extract_br(br, spot, threadId) < 0) {
            cerr << "model extract br info error" << endl;
            return -1;
        }
    } else if (br->site) {
        if (_wap_ctr_model->extract_br(br, spot, threadId) < 0) {
            cerr << "model extract br info error" << endl;
            return -1;
        }
    } else {
        cerr << "app and site is all null!" << endl;
        return -1;
    }

    auto &_cvr_model = _cvr_models[threadId];
    if (_cvr_model) {
        if (_cvr_model->extract_br(br, spot, threadId) < 0) {
             cerr << "app model init error!" << endl;
             return -1;
        }
    }

    auto &_app_model = _app_models[threadId];
    auto &_h5_model = _h5_models[threadId];
    if (_app_model) {
        if (_app_model->extract_br(br, spot, threadId) < 0) {
            cerr << "app model init error!" << endl;
        }
    }
    if (_h5_model) {
        if (_h5_model->extract_br(br, spot, threadId) < 0) {
            cerr << "h5 model init error!" << endl;
        }
    }
    return 0;
}

//裁买逻辑
int BayesBidWorker::do_bidder_purchase(
    std::shared_ptr <BidMax::BidRequest> br,
    AdSpot &spot,
    const BidderCreative &bidCreative,
    string &account_id,
    bidder_result &result,
    const Json::Value &augmentation,
    bool is_purchase,
    int threadId)
{
    auto &_ctr_model = _ctr_models[threadId];
    auto &_h5_model = _h5_models[threadId];
    auto &_app_model = _app_models[threadId];
    auto &_cvr_model = _cvr_models[threadId];
    //auto &_cpm_bidder_model = _cpm_bidder_models[threadId];
    auto &_cpx_bidder_model = _cpx_bidder_models[threadId];
    auto &_wap_ctr_model = _wap_ctr_models[threadId];

    result.reset();

    std::vector<uint32_t> feas_ctr;
    //compute ctr
    if (br->app) {
        result.pctr = _ctr_model->compute_ctr(br, spot, bidCreative, augmentation, threadId, feas_ctr);
    } else if (br->site) {
        result.pctr = _wap_ctr_model->compute_ctr(br, spot, bidCreative, augmentation, threadId, feas_ctr);
    }
    if (result.pctr < 0) {
        return -1;
    }

    std::vector<uint32_t> feas_cvr;
    //computer cvr
    if (bidCreative.is_track_active()){
        result.pcvr = _cvr_model->compute_cvr(br, spot, bidCreative, augmentation, threadId, feas_cvr);
        if(result.pcvr < 0) {
            cerr << "cvr model pcvr error!" << endl;
        }
    }

    result.feas = std::move(feas_ctr);

    switch (bidCreative.bid_method()) {
        case Bid_Method::BID_CPM:
            result.addBidInfoStr("method", "BID_CPM");
            //           _cpm_bidder_model->compute_bid(br, bidCreative, result, is_purchase);
            return _cpx_bidder_model->compute_bid_ex(br, spot, bidCreative, result, "CPM", is_purchase);
        case Bid_Method::BID_CPA:
            result.addBidInfoStr("method", "BID_CPA");
            if (bidCreative.tracking_type() == BidMax::Tracking_Type::BID_MAX) {
                if (_h5_model == nullptr) {
                    result.addBidInfoStr("error", "h5 model error");
                    cerr << "h5 model error" << endl;
                    return -1;
                }
            } else if (bidCreative.tracking_type() == BidMax::Tracking_Type::TALKINGDATA_TRACKING) {
                if (_app_model == nullptr) {
                    result.addBidInfoStr("error", "app model error");
                    cerr << "app model error" << endl;
                    return -1;
                }
            }
            return _cpx_bidder_model->compute_bid_ex(br, spot, bidCreative, result, "CPA", is_purchase);
        case Bid_Method::BID_CPC:
            result.addBidInfoStr("method", "BID_CPC");
            if (_cpx_bidder_model == nullptr) {
                result.addBidInfoStr("error", "_cpx_bidder_mdoel is null");
                cerr << "_cpx_bidder_mdoel is null: " << endl;
                return -1;
            }
            return _cpx_bidder_model->compute_bid_ex(br, spot, bidCreative, result, "CPC", is_purchase);
            //compute pctr
        case Bid_Method::BID_CPT:
            result.addBidInfoStr("method", "BID_CPT");
            if (_cpx_bidder_model == nullptr) {
                result.addBidInfoStr("error", "_cpx_bidder_mdoel is null");
                cerr << "_cpx_bidder_mdoel is null: " << endl;
                return -1;
            }
            return _cpx_bidder_model->compute_bid_ex(br, spot, bidCreative, result, "CPT", is_purchase);
        case Bid_Method::UNSPECIFIED:
            result.addBidInfoStr("method", "UNSPECIFIED");
            cerr << "UNKOWN bid method" << endl;
            break;
        default:
            result.addBidInfoStr("method", "UNKNOWN_" + to_string(bidCreative.bid_method()));
            cerr << "Unkown creative bid_method:" << bidCreative.bid_method() << endl;
            return -1;
    }

    return 0;
}

double BayesBidWorker::get_exchange_ctr(const std::shared_ptr <BidRequest> &br)
{
    if (!this->using_exchange_cpc) {
        return -1;
    }
    string key("");
    if (!br->site && !br->app) {
        cerr << "invalid br->site or br->app! " << endl;
        return this->dft_exchange_ctr;;
    }
    if (br->site) {
        key = br->exchange + "_" + br->site->id.toString();
        key += "_" + br->site->name.rawString();
    } else {
        key = br->exchange + "_" + br->app->id.toString();
        key += "_" + br->app->name.rawString();
    }

    ExchangeCTRDict *exchangeDict = _exchange_ctr_dict_mgr_t->get_content();
    if (exchangeDict == nullptr) {
        cerr << "get exchagne ctr dict error" << endl;
        return this->dft_exchange_ctr;
    }

    double ctr = exchangeDict->get_ctr(key);
    if (ctr < 0) {
        return this->dft_exchange_ctr;
    }
    return ctr;
}

int BayesBidWorker::init(const Json::Value &parameters)
{
    Json::Value reload_tm_json = parameters["reload_time"];
    int reload_tm = static_cast<uint32_t>(reload_tm_json.asInt());

    if (!parameters.isMember("models")) {
        throw ML::Exception("Model setting is not exists!");
    }

    Json::Value models_conf = parameters["models"];

    if (models_conf.isMember("cpm_models")) {
        Json::Value model_conf = models_conf["cpm_models"];
        if (0 != _cpm_bid_mgr.init(model_conf)) {
            throw ML::Exception("init cpm models error");
        }
    }

    if (models_conf.isMember("cpc_models")) {
        Json::Value model_conf = models_conf["cpc_models"];
        if (0 != _cpc_bid_mgr.init(model_conf)) {
            throw ML::Exception("init cpc models error");
        }
    }

    if (models_conf.isMember("cpa_models")) {
        Json::Value model_conf = models_conf["cpa_models"];
        if (0 != _cpa_bid_mgr.init(model_conf)) {
            throw ML::Exception("init cpa models error");
        }
    }

    if (models_conf.isMember("ctr_models")) {
        Json::Value model_conf = models_conf["ctr_models"];
        if (0 != _ctr_model_mgr.init(model_conf)) {
            throw ML::Exception("init ctr models error");
        }
    }

    if (models_conf.isMember("cvr_models")) {
        Json::Value model_conf = models_conf["cvr_models"];
        if (0 != _cvr_model_mgr.init(model_conf)) {
            throw ML::Exception("init cvr models error");
        }
    }

    if (models_conf.isMember("load_models")) {
        Json::Value model_conf = models_conf["load_models"];
        if (0 != _load_model_mgr.init(model_conf)) {
            throw ML::Exception("init load models error");
        }
    }

    if (models_conf.isMember("h5_models")) {
        Json::Value model_conf = models_conf["h5_models"];
        if (0 != _h5_model_mgr.init(model_conf)) {
            throw ML::Exception("init h5 models error");
        }
    }

    if (models_conf.isMember("app_models")) {
        Json::Value model_conf = models_conf["app_models"];
        if (0 != _app_model_mgr.init(model_conf)) {
            throw ML::Exception("init app models error");
        }
    }

    if (models_conf.isMember("cpx_models")) {
        Json::Value model_conf = models_conf["cpx_models"];
        if (0 != _cpx_bid_mgr.init(model_conf)) {
            throw ML::Exception("init cpx models error");
        }
    }

    if (models_conf.isMember("wap_ctr_models")) {
        Json::Value model_conf = models_conf["wap_ctr_models"];
        if (0 != _wap_ctr_model_mgr.init(model_conf)) {
            throw ML::Exception("init wap ctr models error");
        }
    }

    this->dft_exchange_ctr = 0.01;
    if (parameters.isMember("exchange_dft_ctr")) {
        this->dft_exchange_ctr = parameters["exchange_dft_ctr"].asDouble();
    }

    this->using_exchange_cpc = false;
    if (parameters.isMember("using_exchange_cpc")) {
        this->using_exchange_cpc = parameters["using_exchange_cpc"].asBool();
    }

    if (this->using_exchange_cpc) {
        Json::Value exchange_dir = parameters["exchange_ctr_dir"];
        Json::Value exchange_file = parameters["exchange_ctr_file"];
        cout << "exchange_dir: " << exchange_dir.asCString() << " exchange_file: " << exchange_file.asCString() << endl;
        _exchange_ctr_dict_mgr_t = new ExchangeCTRDict_mgr_t(exchange_dir.asCString(),
                                                             exchange_file.asCString(),
                                                             &ExchangeCTRDict_mgr_t::content_type::init);

        if (_exchange_ctr_dict_mgr_t == nullptr) {
            return -1;
        }
        if (_exchange_ctr_dict_mgr_t->reload() < 0) {
            return -1;
        }
    }

    //TODO add cvr
    if (monitor.init(
                reload_tm,
                &_ctr_model_mgr,
                &_cvr_model_mgr,
                &_cpm_bid_mgr,
                &_cpc_bid_mgr,
                &_cpa_bid_mgr,
                &_load_model_mgr,
                &_h5_model_mgr,
                &_app_model_mgr,
                &_cpx_bid_mgr,
                &_wap_ctr_model_mgr) < 0) {
        throw ML::Exception("monitor init error");
    }

    if (parameters.isMember("abtest")) {
        cout << "Begin Load AB....." << endl;
        Json::Value ab_conf = parameters["abtest"];
        Json::Value ab_conf_dir = ab_conf["ab_conf_dir"];
        Json::Value ab_conf_file = ab_conf["ab_conf_file"];

        _ab_config_mgr_t = new AB_config_mgr_t(ab_conf_dir.asCString(),
                                               ab_conf_file.asCString(), &AB_config_mgr_t::content_type::init);

        if (_ab_config_mgr_t == nullptr) {
            cerr << "new _ab_config_mgr_t error " << endl;
            return -1;
        }
        if (_ab_config_mgr_t->reload() < 0) {
            cerr << "Load AB error" << endl;
            return -1;
        }
    } else {
        throw ML::Exception("no abtest in config file");
    }

    monitor.add_AB(_ab_config_mgr_t);

    if (parameters.isMember("constant_bidding")) {
        std::cout << "Begin Load Constant Bidding......" << std::endl;
        Json::Value conf = parameters["constant_bidding"];
        std::string conf_dir = conf["conf_dir"].asString();
        std::string conf_name = conf["conf_name"].asString();

        _constant_bidding_controller_loader = new ConstantBiddingControllerLoader(
                conf_dir.c_str(),
                conf_name.c_str());

        if (_constant_bidding_controller_loader == nullptr) {
            std::cerr << "new _constant_bidding_controller_loader error" << std::endl;
            return -1;
        }

        if (_constant_bidding_controller_loader->reload() < 0) {
            std::cerr << "load constant_bidding_controller error" << std::endl;
            return -1;
        }
    } else {
        throw ML::Exception("no constant_bidding in config file");
    }
    monitor.addConstantBiddingController(_constant_bidding_controller_loader);

    this->using_load_filter = false;
    if (parameters.isMember("using_load_filter")) {
        this->using_load_filter = parameters["using_load_filter"].asBool();
    }
    std::cout << "using load filter: " << this->using_load_filter << std::endl;

    this->load_thr = 0.0;
    if (parameters.isMember("load_thr")) {
        this->load_thr = parameters["load_thr"].asDouble();
    }
    std::cout << "load thr: " << this->load_thr << std::endl;

    this->real_ctr_ratio = 1.2;
    if (parameters.isMember("real_ctr_ratio")) {
        this->real_ctr_ratio = parameters["real_ctr_ratio"].asDouble();
    }
    std::cout << "real ctr ratio: " << real_ctr_ratio << std::endl;

    this->show_threshold = 1000;
    if (parameters.isMember("show_threshold")) {
        this->show_threshold = parameters["show_threshold"].asInt();
    }
    std::cout << "show threshold: " << show_threshold << std::endl;

    std::cout << "Init BayesBidWorker done" << std::endl;

    return monitor.run();
}

int Monitor::init(
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
        CTRModelMgr *_wap_ctr_model_mgr)
{
    reload_time = reload_time_in;

    this->_ctr_model_mgr = _ctr_model_mgr;
    this->_cvr_model_mgr = _cvr_model_mgr;
    this->_cpm_bid_mgr = _cpm_bid_mgr;
    this->_cpc_bid_mgr = _cpc_bid_mgr;
    this->_cpa_bid_mgr = _cpa_bid_mgr;
    this->_load_model_mgr = _load_model_mgr;
    this->_h5_model_mgr = _h5_model_mgr;
    this->_app_model_mgr = _app_model_mgr;
    this->_cpx_bid_mgr = _cpx_bid_mgr;
    this->_wap_ctr_model_mgr = _wap_ctr_model_mgr;

    return 0;
}

void Monitor::add_AB(AB_config_mgr_t *_ab_config_mgr_t)
{
    this->_ab_config_mgr_t = _ab_config_mgr_t;
}

void Monitor::addConstantBiddingController(
        ConstantBiddingControllerLoader * constant_bidding_controller_loader)
{
    this->_constant_bidding_controller_loader = constant_bidding_controller_loader;
}

Monitor::~Monitor()
{
    // do nothing
}

void Monitor::callback()
{
    while (running) {
        if (_ctr_model_mgr->model_reloader() < 0) {
            throw ML::Exception("Reload ctr_model error");
        }
        if (_cvr_model_mgr->model_reloader() < 0) {
            throw ML::Exception("Reload cvr_model error");
        }

        if (_cpm_bid_mgr->model_reloader() < 0) {
            throw ML::Exception("Reload cpm_bid_model error");
        }
        if (_cpc_bid_mgr->model_reloader() < 0) {
            throw ML::Exception("Reload cpc_bid_model error");
        }
        if (_cpa_bid_mgr->model_reloader() < 0) {
            throw ML::Exception("Reload cpa_bid_model error");
        }

        if (_ab_config_mgr_t == nullptr) {
            throw ML::Exception("AB config_mgr is null");
        }
        if (_ab_config_mgr_t->need_reload() == 1) {
            if (_ab_config_mgr_t->reload() < 0) {
                throw ML::Exception("Reload AB error");
            }
        }
        if (_load_model_mgr->model_reloader()) {
            throw ML::Exception("Reload load_model error");
        }
        if (_h5_model_mgr->model_reloader()) {
            throw ML::Exception("Reload h5_model error");
        }
        if (_app_model_mgr->model_reloader()) {
            throw ML::Exception("Reload app_model error");
        }

        if (_cpx_bid_mgr->model_reloader() < 0) {
            throw ML::Exception("Reload cpx_bid_model error");
        }

        if (_wap_ctr_model_mgr->model_reloader() < 0) {
            throw ML::Exception("Reload wap ctr_model error");
        }

        if (_constant_bidding_controller_loader == nullptr) {
            throw ML::Exception("_constant_bidding_controller_loader is null");
        }
        if (_constant_bidding_controller_loader->need_reload() == 1) {
            if (_constant_bidding_controller_loader->reload() < 0) {
                throw ML::Exception("Reload _constant_bidding_controller_loader error");
            }
        }

        sleep(reload_time);
    }
}

void Monitor::join()
{
    pthread_join(thread, nullptr);
}

void Monitor::stop()
{
    running = false;
}

int Monitor::run()
{
    running = true;
    if (0 == pthread_create(&thread, nullptr, &Monitor::_thread, this)) {
        return 0;
    } else {
        return -1;
    }
}

void *Monitor::_thread(void *arg)
{
    Monitor *This = (Monitor *) arg;
    This->callback();
    return nullptr;
}

int AB_config::init()
{
    dis = new std::uniform_int_distribution<>(0, MOD - 1);
    enable_ctr = false;
    enable_cvr = false;
    enable_cpm = false;
    enable_cpa = false;
    enable_cpc = false;
    enable_load = false;
    enable_h5 = false;
    enable_app = false;
    enable_cpx = false;
    enable_wap_ctr = false;
    return 0;
}

int AB_config::load_model_setting(int *model_config, const Json::Value &model_json)
{
    int begin_index = 0;
    int end_index = 0;

    Json::Value::const_iterator iter = model_json.begin();
    while (iter != model_json.end()) {
        const Json::Value &model_conf = *iter;
        Json::Value model_volume_json = model_conf["volume"];
        uint32_t model_volume = model_volume_json.asInt();
        Json::Value model_id_json = model_conf["model_id"];
        uint32_t model_id = static_cast<uint32_t>(model_id_json.asInt());
        end_index = begin_index + model_volume;
        while (begin_index < end_index) {
            model_config[begin_index] = model_id;
            ++begin_index;
        }
        ++iter;
    }
    if (end_index != AB_config::MOD) {
        return -1;
    }
    return 0;
}

int AB_config::load(const char *file_dir, const char *file_name)
{
    if (file_dir == nullptr || file_name == nullptr) {
        return -1;
    }
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);
    Json::Value ab_config_json = loadJsonFromFile(filename);

    if (ab_config_json.isMember("cpc_models")) {
        const Json::Value cpc_json = ab_config_json["cpc_models"];
        if (0 != load_model_setting(cpc_model, cpc_json)) {
            return -1;
        }
        this->enable_cpc = true;
    }

    if (ab_config_json.isMember("cpm_models")) {
        const Json::Value cpm_json = ab_config_json["cpm_models"];
        if (0 != load_model_setting(cpm_model, cpm_json)) {
            return -1;
        }
        this->enable_cpm = true;
    }

    if (ab_config_json.isMember("cpa_models")) {
        const Json::Value cpa_json = ab_config_json["cpa_models"];
        if (0 != load_model_setting(cpa_model, cpa_json)) {
            return -1;
        }
        this->enable_cpa = true;
    }

    if (ab_config_json.isMember("ctr_models")) {
        const Json::Value ctr_json = ab_config_json["ctr_models"];
        if (0 != load_model_setting(ctr_model, ctr_json)) {
            return -1;
        }
        this->enable_ctr = true;
    }

    if (ab_config_json.isMember("cvr_models")) {
        const Json::Value cvr_json = ab_config_json["cvr_models"];
        if (0 != load_model_setting(cvr_model, cvr_json)) {
            return -1;
        }
        this->enable_cvr = true;
    }

    if (ab_config_json.isMember("load_models")) {
        const Json::Value cvr_mix_json = ab_config_json["load_models"];
        if (0 != load_model_setting(load_model, cvr_mix_json)) {
            return -1;
        }
        this->enable_load = true;
    }

    if (ab_config_json.isMember("h5_models")) {
        const Json::Value cvr_mix_json = ab_config_json["h5_models"];
        if (0 != load_model_setting(h5_model, cvr_mix_json)) {
            return -1;
        }
        this->enable_h5 = true;
    }

    if (ab_config_json.isMember("app_models")) {
        const Json::Value cvr_mix_json = ab_config_json["app_models"];
        if (0 != load_model_setting(app_model, cvr_mix_json)) {
            return -1;
        }
        this->enable_app = true;
    }

    if (ab_config_json.isMember("cpx_models")) {
        const Json::Value cpx_json = ab_config_json["cpx_models"];
        if (0 != load_model_setting(cpx_model, cpx_json)) {
            return -1;
        }
        this->enable_cpx = true;
    }

    if (ab_config_json.isMember("wap_ctr_models")) {
        const Json::Value ctr_json = ab_config_json["wap_ctr_models"];
        if (0 != load_model_setting(wap_ctr_model, ctr_json)) {
            return -1;
        }
        this->enable_wap_ctr = true;
    }
    return 0;
}

int AB_config::select_model(
    std::shared_ptr <BidRequest> br,
    model_info_t &model_info)
{
    if (enable_ctr) {
        model_info.ctr_model_id = ctr_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["ctr_id"] = model_info.ctr_model_id;
    }

    if (enable_cvr) {
        model_info.cvr_model_id = cvr_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["cvr_id"] = model_info.cvr_model_id;
    }

    if (enable_cpm) {
        model_info.cpm_model_id = cpm_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["cpm_id"] = model_info.cpm_model_id;
    }

    if (enable_cpa) {
        model_info.cpa_model_id = cpa_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["cpa_id"] = model_info.cpa_model_id;
    }

    if (enable_cpc) {
        model_info.cpc_model_id = cpc_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["cpc_id"] = model_info.cpc_model_id;
    }

    if (enable_load) {
        model_info.load_model_id = load_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["load_id"] = model_info.load_model_id;
    }
    if (enable_h5) {
        model_info.h5_model_id = h5_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["h5_id"] = model_info.h5_model_id;
    }
    if (enable_app) {
        model_info.app_model_id = app_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["app_id"] = model_info.app_model_id;
    }
    if (enable_cpx) {
        model_info.cpx_model_id = cpx_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["cpx_id"] = model_info.cpx_model_id;
    }
    if (enable_wap_ctr) {
        model_info.wap_ctr_model_id = wap_ctr_model[(*dis)(generator) % AB_config::MOD];
        br->bid_model["wap_ctr_id"] = model_info.wap_ctr_model_id;
    }

    return 0;
}

int ExchangeCTRDict::init()
{
    return 0;
}

int ExchangeCTRDict::load(const char *file_dir, const char *file_name)
{
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);
    std::ifstream ifs(filename);
    string line;
    while (getline(ifs, line)) {
        util::str_util::trim(line);
        transform(line.begin(), line.end(), line.begin(), ::tolower);
        vector <string> vec;
        util::str_util::split(line, "\t", vec);
        if (vec.size() < 2) {
            cout << "line " << line << " format error" << endl;
            continue;
        }
        ctr_map[vec[0]] = atof(vec[1].c_str());
    }
    return 0;
}

double ExchangeCTRDict::get_ctr(string key)
{
    transform(key.begin(), key.end(), key.begin(), ::tolower);
    unordered_map<std::string, double>::iterator iter = ctr_map.find(key);
    if (iter != ctr_map.end()) {
        return iter->second;
    }
    return -1;
}

int ConstantBiddingController::init()
{
    return 0;
}

int ConstantBiddingController::load(const char * file_dir, const char * file_name)
{
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);
    std::ifstream ifs(filename);
    std::string line;

    getline(ifs, line);
    util::str_util::trim(line);
    show_threshold = std::stoi(line);
    std::cout << "show_threshold: " << show_threshold << std::endl;

    getline(ifs, line);
    util::str_util::trim(line);
    ratio_threshold = std::stod(line);
    std::cout << "ratio_threshold: " << ratio_threshold << std::endl;

    getline(ifs, line);
    util::str_util::trim(line);
    boost_ratio = std::stod(line);
    std::cout << "boost_ratio: " << boost_ratio << std::endl;

    getline(ifs, line);
    util::str_util::trim(line);
    length = std::stoi(line);

    while (getline(ifs, line)) {
        util::str_util::trim(line);
        if (line.size() == 0) {
            continue;
        }

        try {
            average_bidding_rate.push_back(std::stod(line));
        } catch (...) {
            return -1;
        }
    }

    if (average_bidding_rate.size() != length) {
        return -1;
    }

    return 0;
}

double ConstantBiddingController::bidRatio(
        int c_minute,
        int s_minute,
        int e_minute,
        int count,
        int total_count)
{
    if (total_count == 0) {
        return 0;
    }

    double real_ratio = static_cast<double>(count) / total_count;
    if (real_ratio < ratio_threshold) {
        return 1;
    }

    double expect_ratio = c_minute / length
            + average_bidding_rate[c_minute % length]
            - average_bidding_rate[s_minute];

    expect_ratio /= e_minute / length
            + average_bidding_rate[e_minute % length]
            - average_bidding_rate[s_minute];

    if (expect_ratio == 0) {
        return 0;
    }

    double bid_ratio = 1;
    if (real_ratio > expect_ratio) {
        bid_ratio = exp(1 - real_ratio / expect_ratio);
    }
    bid_ratio += boost_ratio;
    
    return bid_ratio;
}

} /* namespace bayes */
