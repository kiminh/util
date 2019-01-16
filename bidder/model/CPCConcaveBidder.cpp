#include "CPCConcaveBidder.h"
#include "soa/jsoncpp/json.h"
#include <math.h>
#include <iostream>

namespace bayes
{
using namespace std;

CPCConcaveBidder::CPCConcaveBidder(int id)
{
    model_name = "CPCConcaveBidder";
    model_id = id;
}

CPCConcaveBidder::~CPCConcaveBidder()
{
    // do nothing
}

int CPCConcaveBidder::init(const Json::Value &parameters)
{
    this->clk_thr = 0;
    if (parameters.isMember("clk_thr")) {
        Json::Value clk_json = parameters["clk_thr"];
        this->clk_thr = clk_json.asUInt();
    }
    cout << "clk_thr: " << this->clk_thr << endl;

    this->lambda_scale = 1000000;
    if (parameters.isMember("lambda_scale")) {
        Json::Value scale_json = parameters["lambda_scale"];
        this->lambda_scale = scale_json.asInt();
    }
    cout << "lambda_scale: " << this->lambda_scale << endl;

    this->default_aduser = 0;
    if (parameters.isMember("default_aduser")) {
        Json::Value dft_ad_json = parameters["default_aduser"];
        this->default_aduser = dft_ad_json.asInt();
    }
    cout << "default aduser: " << this->default_aduser << endl;

    Json::Value track_bidder_rate_json = parameters["tracking_bidder_rate"];
    this->tracking_bidder_rate = track_bidder_rate_json.asDouble();
    cout << "tracking bidder_rate : " << tracking_bidder_rate << endl;

    Json::Value notrack_bidder_rate_json = parameters["notracking_bidder_rate"];
    this->notracking_bidder_rate = notrack_bidder_rate_json.asDouble();
    cout << "notracking bidder_rate : " << notracking_bidder_rate << endl;

    Json::Value bid_dir = parameters["bid_dir"];
    Json::Value bid_file = parameters["bid_file"];
    cout << "bid_dir: " << bid_dir.asCString() << " bid_file: " << bid_file.asCString() << endl;
    _aduser_bid_mgr_t = new BidDict_mgr_t(bid_dir.asCString(),
                                          bid_file.asCString(), &BidDict_mgr_t::content_type::init);

    if (_aduser_bid_mgr_t == nullptr) {
        throw ML::Exception("new aduser bid dict error!");
    }
    return 0;
}

int CPCConcaveBidder::compute_bid(
    std::shared_ptr <BidMax::BidRequest> br,
    const BidMax::AdSpot &spot,
    const BidderCreative &bidCreative,
    bidder_result &result,
    bool is_purchase)
{
    BidDict *bid_dict = _aduser_bid_mgr_t->get_content();
    if (bid_dict == nullptr) {
        cerr << "get bid para dict error" << endl;
        return -1;
    }

    uint32_t post_clk = 0;
    double ad_lambda;
    double ad_c;
    bid_dict->get_clk(bidCreative.ads_user_id(), post_clk);
    int ret = bid_dict->get_paras(bidCreative.ads_user_id(), ad_lambda, ad_c);

    double dft_lambda;
    double dft_c;
    int ret2 = bid_dict->get_paras(this->default_aduser, dft_lambda, dft_c);

    if (ret != 0 && ret2 != 0) {
        cerr << "no userid in dict,cannot compute bid" << endl;
        return -1;
    }

    if (post_clk >= this->clk_thr || ret != 0) {
        ad_lambda = dft_lambda;
        ad_c = dft_c;
    }
    if (bidCreative.tracking_type() != BidMax::Tracking_Type::NOTRACKING) {
        result.price = calc_bid(ad_lambda, ad_c, result.pcvr * result.pctr, lambda_scale) * tracking_bidder_rate;
    } else {
        result.price =
            calc_bid(dft_lambda, dft_c, result.pctr * result.medium_pvalue, lambda_scale) * notracking_bidder_rate;
    }
    result.priority = result.price;
    return 0;
}

double CPCConcaveBidder::calc_bid(double lambda, double c, double pvalue, int lambda_scale)
{
    return sqrt(c / lambda * lambda_scale * pvalue + c * c) - c;
}

int CPCConcaveBidder::model_reloader()
{
    if (_aduser_bid_mgr_t == nullptr) {
        return -1;
    }
    if (_aduser_bid_mgr_t->need_reload() == 1) {
        if (_aduser_bid_mgr_t->reload() < 0) {
            return -1;
        }
    }
    return 0;
}

int BidDict::init()
{
    return 0;
}

int BidDict::load(const char *file_dir, const char *file_name)
{
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);
    std::ifstream model_file(filename);

    string line;
    //cout << "show thr " << show_thr << endl;
    while (getline(model_file, line)) {
        util::str_util::trim(line);
        transform(line.begin(), line.end(),
                  line.begin(), ::tolower);
        vector <string> vec;
        util::str_util::split(line, "\t", vec);
        //1       1.172774        2741.290316     56944
        //lemda c
        if (vec.size() < 4) {
            cout << "line " << line << " format error" << endl;
            continue;
        }
        //uint32_t clk = strtoul(vec[3].c_str(), NULL, 0);
        int ad_user = atoi(vec[0].c_str());
        double lambda = atof(vec[1].c_str());
        double c = atof(vec[2].c_str());

        uint32_t clk = strtoul(vec[3].c_str(), NULL, 0);
        pair<double, double> tmp;

        aduser_bid_dict[ad_user] = pair<double, double>(lambda, c);
        aduser_clk_dict[ad_user] = clk;
    }
    cout << "load into bid_dict : " << aduser_bid_dict.size() << "\t" << aduser_clk_dict.size() << endl;
    return 0;
}

int BidDict::get_clk(int aduser, uint32_t &clk)
{
    unordered_map<int, uint32_t>::iterator iter = aduser_clk_dict.find(aduser);
    clk = 0;
    if (iter != aduser_clk_dict.end()) {
        clk = iter->second;
    }
    return clk;
}

int BidDict::get_paras(int aduser, double &lambda, double &c)
{
    unordered_map < int, pair < double, double > > ::iterator
    iter = aduser_bid_dict.find(aduser);
    if (iter != aduser_bid_dict.end()) {
        pair<double, double> ret = iter->second;
        lambda = ret.first;
        c = ret.second;
        return 0;
    }
    return -1;
}

} /* namespace bayes */
