#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include <cmath>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "bidmax/common/bids.h"
#include "soa/service/service_utils.h"
#include "jml/utils/file_functions.h"
#include "soa/service/dual_reloader.h"
#include "bayes_bidding_agent_ex.h"

using namespace std;
using namespace ML;
using namespace bayes;

namespace BidMax
{

#define FATAL_LOG(...) \
    logMessage("FATAL", __FILE__, __LINE__, __func__, serviceName(), __VA_ARGS__)

#define WARNING_LOG(...) \
    logMessage("WARNING", __FILE__, __LINE__, __func__, serviceName(), __VA_ARGS__)

#define DEBUG_LOG(...) \
    logMessage("DEBUG", __FILE__, __LINE__, __func__, serviceName(), __VA_ARGS__)

static inline Json::Value loadJsonFromFile(const std::string &filename)
{
    ML::File_Read_Buffer buf(filename);
    return Json::parse(std::string(buf.start(), buf.end()));
}

void BayesBiddingAgent::init(std::string & bidder_worker_config)
{
    BiddingAgent::init(num_threads);

    int ret = initLog();
    if (ret == 0) {
        WARNING_LOG("bidder log init OK!");
    } else {
        FATAL_LOG("init log failed!");
    }

    handleRequest = [&] (BiddingRequest & request)
    {
        bid(request);
    };

    onMsgDebug = [&] (const std::string & msg)
    {
        DEBUG_LOG(msg);
    };

    onMsgWarning = [&] (const std::string & msg)
    {
        WARNING_LOG(msg);
    };

    onMsgFatal = [&] (const std::string & msg)
    {
        FATAL_LOG(msg);
    };

    //bidder.json
    initWorker(bidder_worker_config);
}

void BayesBiddingAgent::initWorker(std::string &bidder_worker_config)
{
    //bid conf
    Json::Value bidder_worker_conf_json = loadJsonFromFile(bidder_worker_config);

    if (bidder_worker_conf_json.isNull()) {
        throw ML::Exception("Load bidder_worker_conf is null");
    }

    if (0 != bayes_bid_worker.init(bidder_worker_conf_json)) {
        throw ML::Exception("bayes_bid_worker init error");
    }
}

bool BayesBiddingAgent::start()
{
    BiddingAgent::start();
    _logger_client.start();
    return true;
}

void BayesBiddingAgent::shutdown()
{
    BiddingAgent::shutdown();
    _logger_client.shutdown();
}

int BayesBiddingAgent::initLog()
{
    _logger_client.onConnected = [&] (const std::string & server)
    {
        // do nothing
    };

    _logger_client.onResponse = [=] (
            const std::shared_ptr<BayesBiddingAgent> & info,
            const std::vector<std::string> & message)
    {
        // do nothing
    };

    _logger_client.onWarning = [=] (const std::string & msg)
    {
        // do nothing
    };

    _logger_client.onDebug = [=] (const std::string & msg)
    {
        // do nothing
    };

    _logger_client.onFatal = [=] (const std::string & msg)
    {
        // do nothing
    };

    _logger_client.init(
            getServices()->config,
            serviceName(),
            "BayesLogger",
            "logger");

    return 0;
}

void BayesBiddingAgent::optimizePrice(
        Bid & bid,
        bayes::bidder_result & result,
        std::shared_ptr<BidMax::BidRequest> br,
        const BidderCreatives_BidderCreative & creative)
{
    bool is_purchase = creative.private_exchange() != br->exchange;
    int bid_method = creative.bid_method();

    if (!is_purchase) {
        if (result.price < 1) {
            WARNING_LOG(ML::format(
                    "fill/bottom price too low(<1) for (%s, %d) with %f.",
                    br->auctionId.toString(),
                    creative.id(),
                    result.price));
            result.price = 1;
        }
    }

    if (bid_method == Bid_Method::BID_CPC) {
        if (result.pctr < creative.self_buy_cut_ctr()) {
            result.price = 0;
            result.priority = 0;
            result.addBidInfoStr("reason", "CtrCut");
            bid.no_bid_reason = "CtrCut";
            return;
        }
    }

    if (is_purchase) {
        //kanli
        int bid_floor = 99999;
        if (creative.adx_adspot_id() == -1) {
            // non-adx
            bid_floor = static_cast<int>(creative.cpm_for_sell() * 1000);
        } else {
            // adx
            float adx_rating_ratio = creative.adx_rating_ratio();
            bid_floor = br->imp[bid.spotIndex].bidfloor.val * adx_rating_ratio;
        }

        switch (bid_method) {
            case Bid_Method::BID_CPC:
            case Bid_Method::BID_CPA:
            case Bid_Method::BID_CPM:
                //bid_floor filter
                if(result.price < bid_floor) {
                    result.price = 0;
                    result.priority = 0;
                    result.addBidInfoStr("reason", "BidFloor");
                    bid.no_bid_reason = "BidFloor";
                    return;
                }
                break;
            case Bid_Method::BID_CPT:
                break;
            default:
                FATAL_LOG(ML::format("invalid bid method %d.", bid_method));
                break;
        }
    }

    if (bayes_bid_worker.isBid(creative, result.pctr) == 0) {
        result.price = 0;
        result.priority = 0;
        result.addBidInfoStr("reason", "ConstantBidding");
        bid.no_bid_reason = "ConstantBidding";
        return;
    }
}

int BayesBiddingAgent::doActualBid(
    std::shared_ptr<BidMax::BidRequest> br,
    const vector<uint16_t> & index_list,
    Bid & bid,
    AdSpot & spot,
    const Json::Value & augmentation,
    Json::Value & bidDebugInfos,
    int thread_index,
    std::shared_ptr<BidderCreatives> bidder_creatives)
{
    bool is_purchase = false;
    bool is_adx = false;
    int cpm_for_sell = 0;
    for (int i = 0; i < index_list.size(); ++i) {
        auto & bid_creative = bidder_creatives->creatives(index_list[i]);

        // CPT/bottom belong to self buy
        is_purchase = bid_creative.private_exchange() != br->exchange;
        is_adx = bid_creative.adx_adspot_id() != -1;
        cpm_for_sell = static_cast<int>(bid_creative.cpm_for_sell() * 1000);

        bayes::bidder_result result;
        result.setTest(br->isLog);

        int ret = bayes_bid_worker.do_bidder_purchase(
                br,
                spot,
                bid_creative,
                account,
                result,
                augmentation,
                is_purchase,
                thread_index);

        if (0 != ret) {
            WARNING_LOG(ML::format(
                    "Bid Fail for (%s, %d).",
                    br->auctionId.toString(),
                    bid_creative.id()));
            // bid.bid_bayes(bid_creative.creative_index(), MicroCNY(0), result);
            bid.bid_bayes_new(bid_creative.creative_index(), bid_creative.company_id(), MicroCNY(0), result);
            continue;
        }

        optimizePrice(bid, result, br, bid_creative);

        // bid.bid_bayes(bid_creative.creative_index(), MicroCNY(result.price), result);
        bid.bid_bayes_new(bid_creative.creative_index(), bid_creative.company_id(), MicroCNY(result.price), result);

        if (br->isLog) {
            bidDebugInfos[to_string(bid_creative.id())]["infos"] = result.toJson();
        }
    }

    bid.calGSP(is_purchase, cpm_for_sell, is_adx);

    if (bid.isNullBid()) {
        bid.reset();
        return -1;
    } else {
        logMessage("FEA", br->auctionId.toString(), bid.feaToStr());
        return 0;
    }
}

void BayesBiddingAgent::bid(BiddingRequest & request)
{
    std::shared_ptr<BidMax::BidRequest> br = request.bid_request;
    Bids & bids = request.bids;
    Json::Value & augmentation = request.augmentation;
    std::shared_ptr<BidderCreatives> bidder_creatives = request.bidder_creatives;
    int thread_index = request.thread_index;

    if ((br->site && br->app) || (!br->site && !br->app)) {
        return;
    }

    // log debug infos while bidding when isLog == true
    Json::Value debugInfos;
    bool isLog = br->isLog;
    bool is_purchase_first =
            br->self_purchase_priority < br->purchase_priority ? false : true;

    if (isLog) {
        debugInfos["exchange_name"] = "Exchange: " + br->exchange;
    }
    //select model to do bid
    bayes::model_info_t model_info;

    if (bayes_bid_worker.select_model(br, model_info)) {
        throw ML::Exception("Model select is error");
    }

    if (bayes_bid_worker.set_model_info(model_info, thread_index) != 0) {
        throw ML::Exception("Model set is error");
    }

    string fea_pctr("");
    string fea_pcvr("");
    string fea_pload("");
    string native_info_str;
    string has_show_str;
    string is_track_active_str;

    for (Bid &bid : bids) {
        uint16_t availableSize = bid.availableCreatives.size();
        if (0 == availableSize) {
            continue;
        }

        Json::Value bidDebugInfos = {};
        AdSpot &spot = br->imp[bid.spotIndex];

        bayes_bid_worker.init_br_info(br, spot, thread_index);

        std::vector <uint16_t> purchase_index_list;
        std::vector <uint16_t> fill_index_list;
        std::vector <uint16_t> bottom_index_list;
        std::vector <uint16_t> cpt_index_list;

        for (uint16_t index = 0; index < bidder_creatives->creatives_size(); ++index) {
            auto & bidder_creative = bidder_creatives->creatives(index);

            if (bidder_creative.creative_valid() != 1) {
                continue;
            }

            bool is_biddable = false;
            for (auto index : bid.availableCreatives) {
                if (index == bidder_creative.creative_index()) {
                    is_biddable = true;
                    break;
                }
            }

            if (!is_biddable) {
                WARNING_LOG("not compatible with this bid",
                        "auction_id",
                        br->auctionId.toString(),
                        "bid",
                        bid.spotIndex,
                        "creative_id",
                        bidder_creative.id());
                continue;
            }

            if (isLog) {
                Json::Value bidInfo;
                bidInfo["exchange_id"] = br->exchange;
                bidInfo["creative_id"] = bidder_creative.id();
                bidInfo["bid_type"] = bidder_creative.bid_method();
                bidInfo["is_bottom"] = bidder_creative.is_bottom();
                bidInfo["is_self_buy"] = bidder_creative.private_exchange() == br->exchange;
                bidDebugInfos[to_string(bidder_creative.id())] = bidInfo;
            }

            if (bidder_creative.bid_method() == Bid_Method::BID_CPT) {
                cpt_index_list.push_back(index);
                continue;
            }

            if (bidder_creative.is_bottom()) {
                bottom_index_list.push_back(index);
                continue;
            }

            if (bidder_creative.private_exchange() == br->exchange) {
                fill_index_list.push_back(index);
            } else {
                purchase_index_list.push_back(index);
            }
        }

        // cpt first
        int bid_result = doActualBid(br, cpt_index_list, bid, spot, augmentation, bidDebugInfos, thread_index, bidder_creatives);

        if (bid_result != 0 && is_purchase_first) {
            bid_result = doActualBid(br, purchase_index_list, bid, spot, augmentation, bidDebugInfos, thread_index, bidder_creatives);
        }

        if (bid_result != 0) {
            bid_result = doActualBid(br, fill_index_list, bid, spot, augmentation, bidDebugInfos, thread_index, bidder_creatives);
        }

        if (bid_result != 0 && !is_purchase_first) {
            bid_result = doActualBid(br, purchase_index_list, bid, spot, augmentation, bidDebugInfos, thread_index, bidder_creatives);
        }

        if (bid_result != 0) {
            bid_result = doActualBid(br, bottom_index_list, bid, spot, augmentation, bidDebugInfos, thread_index, bidder_creatives);
        }

        fea_pctr += "_" + to_string(bid.predict_ctr);
        fea_pcvr += "_" + to_string(bid.predict_cvr);
        fea_pload += "_" + to_string(bid.predict_load);

        int bid_creative_id = -1;

        for (int index = 0; index < bidder_creatives->creatives_size(); index++) {
            auto &bidder_creative = bidder_creatives->creatives(index);
            if (bidder_creative.creative_index() != bid.creativeIndex) {
                continue;
            }

            bid_creative_id = bidder_creative.id();
            std::string json_str = "";
            if (bidder_creative.has_native_info()) {
                BidMax::transform_native_to_json(bidder_creative.native_info(), json_str);
            } else {
                Json::Value native_json;
                native_json["native_config_id"] = -1;
                native_json["image"] = Json::Value();
                native_json["word"] = Json::Value();
                native_json["video"] = Json::Value();
                json_str = native_json.toString();
            }

            native_info_str += "_" + json_str;
            has_show_str += "_" + to_string(bidder_creative.has_show());
            is_track_active_str += "_" + to_string(bidder_creative.is_track_active());

            // not valid for another bid to bid
            // add this for multiple impressions
            auto mutable_bid_creative = bidder_creatives->mutable_creatives(index);
            mutable_bid_creative->set_creative_valid(0);
        }

        if (isLog) {
            if (bid_creative_id != -1) {
                bidDebugInfos[to_string(bid_creative_id)]["infos"]["bid_info"]["reason"] = "BidWin";
            }
            debugInfos[to_string(bid.spotIndex)] = bidDebugInfos;
        }
    }

    Json::Value metadata = br->bid_model;

    if (!fea_pctr.empty()) {
        fea_pctr = fea_pctr.substr(1);
    }
    metadata["pctr"] = fea_pctr;

    if (!fea_pcvr.empty()) {
        fea_pcvr = fea_pcvr.substr(1);
    }
    metadata["pcvr"] = fea_pcvr;

    if (!fea_pload.empty()) {
        fea_pload = fea_pload.substr(1);
    }
    metadata["pload"] = fea_pload;

    if (!native_info_str.empty()) {
        native_info_str = native_info_str.substr(1);
    }
    metadata["native_info"] = native_info_str;

    if (!has_show_str.empty()) {
        has_show_str = has_show_str.substr(1);
    }
    metadata["has_show"] = has_show_str;

    if (!is_track_active_str.empty()) {
        is_track_active_str = is_track_active_str.substr(1);
    }
    metadata["is_track_active"] = is_track_active_str;

    metadata["ext"] = br->ext.toString();

    int is_wap = 0;
    if (br->site) {
        is_wap = 1;
    }
    metadata["wap"] = is_wap;

    if (augmentation.isMember("data")) {
        Json::Value data = augmentation["data"];
        try {
            Json::Value freq_data;
            freq_data["total"] = data["total"].asUInt();
            if (data["cid"].isArray() && data["freq_data"].isArray()) {
                for (Json::Value::UInt i = 0; i < data["cid"].size(); ++i) {
                    Json::Value cid_data;
                    cid_data["freq_cid"] = data["freq_data"][i]["cid"].asInt();
                    cid_data["freq_adid"] = data["freq_data"][i]["ad_id"].asInt();
                    int cid = data["cid"][i].asInt();
                    freq_data[to_string(cid)] = cid_data;
                }
            }

            // data["daily_freq_cap"]
            // data["freq_cap"]
            // "freq_cap":{"1000757":{"100002097":{"BID":0,"WIN":0,"CLICK":0}}}
            // if (data.isMember("freq_cap")) {
            //     std::cerr << "freq_cap: " << data["freq_cap"].toString() << std::endl;
            // }
            metadata["augments"] = freq_data;
        } catch (...) {
            FATAL_LOG("Parse byc-augmentor json data failed.");
        }
    }

    if (isLog) {
        metadata["bidderDebugInfos"] = debugInfos;
    }

    request.meta = std::move(metadata);

    // Send our bid back to the agent.
    respond(request);
}

}
