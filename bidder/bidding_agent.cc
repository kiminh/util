#include "bidding_agent.h"

#include "jml/arch/exception.h"
#include "jml/arch/timers.h"
#include "jml/utils/vector_utils.h"
#include "jml/utils/exc_check.h"
#include "jml/utils/exc_assert.h"
#include "jml/arch/futex.h"
#include "soa/service/zmq_utils.h"
#include "soa/service/process_stats.h"

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <iostream>

using namespace std;
using namespace BAYESCOM;
using namespace BidMax;
using namespace ML;

namespace BidMax {

static Json::Value jsonParse(const std::string & str)
{
    if (str.empty()) {
        return Json::Value();
    }
    return Json::parse(str);
}

BiddingAgent::BiddingAgent(
        std::shared_ptr<ServiceProxies> proxies,
        const std::string & name)
    : ServiceBase(name, proxies)
    , _agent_name(name)
    , _network_server(name, getZmqContext())
{
    onMsgDebug = [&] (const std::string & msg)
    {
        std::cerr << "BIDDER DEBUG: " << msg << std::endl;
    };

    onMsgWarning = [&] (const std::string & msg)
    {
        std::cerr << "BIDDER WARNING: " << msg << std::endl;
    };

    onMsgFatal = [&] (const std::string & msg)
    {
        std::cerr << "BIDDER FATAL: " << msg << std::endl;
    };
}

BiddingAgent::BiddingAgent(
        ServiceBase& parent,
        const std::string & name)
    : ServiceBase(name, parent)
    , _agent_name(name)
    , _network_server(name, getZmqContext())
{
    onMsgDebug = [&] (const std::string & msg)
    {
        std::cerr << "BIDDER DEBUG: " << msg << std::endl;
    };

    onMsgWarning = [&] (const std::string & msg)
    {
        std::cerr << "BIDDER WARNING: " << msg << std::endl;
    };

    onMsgFatal = [&] (const std::string & msg)
    {
        std::cerr << "BIDDER FATAL: " << msg << std::endl;
    };
}

BiddingAgent::~BiddingAgent()
{
    shutdown();
}

void BiddingAgent::init(int num_threads)
{
    registerServiceProvider(serviceName(), {"BayesBidder"});
    _network_server.init(
            getServices()->config,
            serviceName() + "/bidders",
            num_threads);
    _network_server.bindTcp(getServices()->ports->getRange("bidders"));

    _network_server.onRequest = [&] (const Message & msg, int thread_index)
    {
        BiddingRequest request;
        parseMessage(request, msg, thread_index);
        handleRequest(request);
    };

    _network_server.onConfig = [&] (const std::vector<std::string> & msg)
    {
        // do nothing
    };

    _network_server.onWarning = [&] (const std::string & msg)
    {
        onMsgWarning(msg);
    };

    _network_server.onDebug = [&] (const std::string & msg)
    {
        onMsgDebug(msg);
    };

    _network_server.onFatal = [&] (const std::string & msg)
    {
        onMsgFatal(msg);
    };

    addSource("BiddingAgent::networkServer", _network_server);
}

void BiddingAgent::start()
{
    MessageLoop::start();
}

void BiddingAgent::shutdown()
{
    MessageLoop::shutdown();
}

void BiddingAgent::respond(const BiddingRequest & req)
{
    Json::FastWriter jsonWriter;

    std::string bids_str = jsonWriter.write(req.bids.toJson());
    boost::trim(bids_str);

    std::string meta_str = jsonWriter.write(req.meta);
    boost::trim(meta_str);

    std::vector<std::string> payload = {
            req.router,
            "RESPONSE",
            req.id.toString(),
            bids_str,
            meta_str,
            _agent_name};

    _network_server.addEntry(payload);
}

void BiddingAgent::parseMessage(
        BiddingRequest & request,
        const Message & message,
        int thread_index)
{
    request.router = message.first;
    request.id = Id(message.second[2]);

    const string & brSource = message.second[3];
    const string & brStr = message.second[4];
    request.bid_request.reset(BidRequest::parse(brSource, brStr));

    Json::Value imp = jsonParse(message.second[5]);
    Bids bids;
    bids.reserve(imp.size());
    for (size_t i = 0; i < imp.size(); ++i) {
        Bid bid;

        bid.spotIndex = imp[i]["spot"].asInt();
        for (const auto& creative : imp[i]["creatives"]) {
            bid.availableCreatives.push_back(creative.asInt());
        }

        bids.push_back(bid);
    }
    request.bids = std::move(bids);

    request.augmentation = jsonParse(message.second[6]);
    std::shared_ptr<BidderCreatives> bidder_creatives_ptr(new BidderCreatives());
    if (!bidder_creatives_ptr->ParseFromString(message.second[7])) {
        std::cerr << "bidder creatives parse failed!!!" << std::endl;
        return;
    }
    request.bidder_creatives = bidder_creatives_ptr;

    request.thread_index = thread_index;
}

} // namespace BidMax
