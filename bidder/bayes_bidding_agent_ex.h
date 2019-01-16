#include "BayesBidWorker.h"
#include "bidding_agent.h"
#include "bidmax/common/bids.h"
#include "bidmax/common/bidder_creative_transform.h"
#include "bidmax/common/network_client.h"
#include "soa/service/service_utils.h"
#include "soa/service/zmq_named_pub_sub.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <string>

#define RANDOM_MAX 71

using namespace std;
using namespace ML;
using namespace bayes;

namespace BidMax {
class BayesBiddingAgent : public BiddingAgent {
public:
    BayesBiddingAgent(
            std::shared_ptr <BAYESCOM::ServiceProxies> services,
            const string &serviceName)
        : BiddingAgent(services, serviceName)
        , _logger_client("BayesBiddingAgent")
        , num_threads(4)
    {
        // do nothing
    }

    int initLog();

    void init(std::string & bidder_worker_config);

    void initWorker(std::string &bidder_worker_config);

    bool start();

    void shutdown();

    void bid(BiddingRequest & request);

    void optimizePrice(
            Bid & bid,
            bayes::bidder_result & result,
            std::shared_ptr<BidMax::BidRequest> br,
            const BidderCreatives_BidderCreative & creative);

    int doActualBid(
            std::shared_ptr<BidMax::BidRequest> br,
            const vector<uint16_t> & index_list,
            Bid & bid,
            AdSpot & spot,
            const Json::Value & augmentation,
            Json::Value & bidDebugInfos,
            int thread_index,
            std::shared_ptr<BidderCreatives> bidder_creatives);

    template<typename Head, typename... Tail>
    void encodeAll(
            std::vector<zmq::message_t> & messages,
            Head head,
            Tail&&... tail)
    {
        messages.emplace_back(std::move(encodeMessage(head)));
        encodeAll(messages, std::forward<Tail>(tail)...);
    }

    // Vectors treated specially... they are copied
    template<typename... Tail>
    void encodeAll(
            std::vector<zmq::message_t> & messages,
            const std::vector<std::string> & head,
            Tail&&... tail)
    {
        for (auto & m: head) {
            messages.emplace_back(std::move(encodeMessage(m)));
        }
        encodeAll(messages, std::forward<Tail>(tail)...);
    }

    void encodeAll(std::vector<zmq::message_t> & messages)
    {
        // do nothing
    }

    template<typename... Args>
    void logMessage(const std::string &channel, Args... args)
    {
        std::vector<zmq::message_t> messages;
        messages.reserve(sizeof...(Args) + 2);
        encodeAll(messages, channel, Date::now().print(5), std::forward<Args>(args)...);
        _logger_client.addLogger(messages);
    }

public:
    std::string account;
    std::vector <std::string> exchange_name;
    Json::Value bidderConfigJson;
    bayes::BayesBidWorker bayes_bid_worker;
    BidMax::NetworkClient<BayesBiddingAgent> _logger_client;

    bool using_exchange_cpc;

    int num_threads;

};

}
