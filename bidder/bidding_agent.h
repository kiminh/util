#ifndef __rtb__bidding_agent_h__
#define __rtb__bidding_agent_h__


#include "bidmax/common/auction.h"
#include "bidmax/common/bids.h"
#include "bidmax/common/auction_events.h"
#include "bidmax/common/win_cost_model.h"
#include "soa/service/zmq.hpp"
#include "soa/service/carbon_connector.h"
#include "soa/jsoncpp/json.h"
#include "soa/types/id.h"
#include "soa/service/service_base.h"
#include "soa/service/zmq_endpoint.h"
#include "soa/service/typed_message_channel.h"

#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/make_shared.hpp>
#include <string>
#include <vector>
#include <thread>
#include <map>

#include "bidmax/common/network_server.h"
#include "bidmax/common/bidder_creative.pb.h"

#include <boost/function.hpp>
#include <boost/thread.hpp>

namespace BidMax {

struct BiddingAgent : public ServiceBase, public MessageLoop {
public:
    struct BiddingRequest
    {
        std::string router;
        Id id;
        std::shared_ptr<BidRequest> bid_request;
        Bids bids;
        Json::Value augmentation;
        std::shared_ptr<BidderCreatives> bidder_creatives;
        int thread_index;

        Json::Value meta;
    };

    typedef boost::function<void (const std::string & msg)> OnMsgLog;
    typedef boost::function<void (BiddingRequest &)> RequestHandle;
    typedef std::pair<std::string, std::vector<std::string> > Message;

public:
    BiddingAgent(
            std::shared_ptr<ServiceProxies> proxies,
            const std::string & name);

    BiddingAgent(
            ServiceBase & parent,
            const std::string & name);

    ~BiddingAgent();

    void init(int num_threads = 1);

    void start();

    void shutdown();

    void respond(const BiddingRequest & req);

private:
    void parseMessage(
            BiddingRequest & req,
            const Message & msg,
            int thread_id);

public:
    OnMsgLog onMsgDebug;
    OnMsgLog onMsgWarning;
    OnMsgLog onMsgFatal;

protected:
    RequestHandle handleRequest;

private:
    std::string _agent_name;

    NetworkServer _network_server;
};

} // namespace BidMax

#endif // __rtb__bidding_agent_h__
