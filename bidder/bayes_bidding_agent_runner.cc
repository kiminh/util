#include "bayes_bidding_agent_ex.h"
#include "soa/service/service_utils.h"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <iostream>
#include <thread>
#include <chrono>

using namespace std;

int main(int argc, char **argv)
{
    using namespace boost::program_options;

    BAYESCOM::ServiceProxyArguments args;
    std::string bidderWorkerConf;

    options_description options = args.makeProgramOptions();

    options.add_options()("worker,C", value<string>(&bidderWorkerConf),
                          "configuration file with feature extract");
    options.add_options()("help,h", "Print this message");

    variables_map vm;
    store(command_line_parser(argc, argv).options(options).run(), vm);
    notify(vm);

    if (vm.count("help")) {
        cerr << options << endl;
        return 1;
    }
    srand((int) time(0));

    auto serviceProxies = args.makeServiceProxies();
    auto serviceName = args.serviceName("BayesBidder");
    BidMax::BayesBiddingAgent agent(serviceProxies, serviceName);

    agent.init(bidderWorkerConf);
    agent.start();

    while (true) {
        this_thread::sleep_for(chrono::seconds(10));
    }

    // Won't ever reach this point but this is how you shutdown an agent.
    agent.shutdown();

    return 0;
}
