#include "CPXLRBidder.h"
#include "soa/jsoncpp/json.h"
#include <math.h>

namespace bayes
{
using namespace BidMax;
using namespace std;

CPXLRBidder::CPXLRBidder(int id)
{
    model_name = "CPXLRBidder";
    model_id = id;
    _ad_cvr_dict_mgr_t = nullptr;
    dft_target_cvr = 0.001684;
    dft_target_cpa = 0.4;
}

CPXLRBidder::~CPXLRBidder()
{
    // do nothing
}

int CPXLRBidder::init(const Json::Value &parameters)
{
    max_trust_ctr = 0.8;
    if (parameters.isMember("max_trust_ctr")) {
        Json::Value max_trust_ctr_json = parameters["max_trust_ctr"];
        max_trust_ctr = max_trust_ctr_json.asDouble();
    }
    //cut_ctr
    cut_ctr = 0.005;
    if (parameters.isMember("cut_ctr")) {
        Json::Value cut_ctr_json = parameters["cut_ctr"];
        cut_ctr = cut_ctr_json.asDouble();
    }

    bidder_rate = 1.0;
    if (parameters.isMember("bidder_rate")) {
        Json::Value bidder_rate_json = parameters["bidder_rate"];
        bidder_rate = bidder_rate_json.asDouble();
    }

    use_cvr = false;
    if (parameters.isMember("use_cvr")) {
        use_cvr = parameters["use_cvr"].asBool();
    }
    cout << "use_cvr: " << use_cvr << endl;

    cvr_odd_ratio = 1;
    if (parameters.isMember("cvr_odd_ratio")) {
        cvr_odd_ratio = parameters["cvr_odd_ratio"].asDouble();
        cout << "cvr_odd_ratio: " << cvr_odd_ratio << endl;
    }

    if (use_cvr) {
        //读取ad user 相关cvr
        Json::Value cvr_dir = parameters["ad_cvr_dir"];
        Json::Value cvr_file = parameters["ad_cvr_file"];
        cout << "adcvr_dir: " << cvr_dir.asCString() << " adcvr_file: " << cvr_file.asCString() << endl;
        _ad_cvr_dict_mgr_t = new AdCVRDict_mgr_t(
                cvr_dir.asCString(),
                cvr_file.asCString(),
                &AdCVRDict_mgr_t::content_type::init);

        if (_ad_cvr_dict_mgr_t == nullptr) {
            return -1;
        }
        if (_ad_cvr_dict_mgr_t->reload() < 0) {
            return -1;
        }
    }

    use_target_cpa = false;
    if (parameters.isMember("use_target_cpa")) {
        use_target_cpa = parameters["use_target_cpa"].asBool();
    }
    cout << "use_target_cpa: " << use_target_cpa << endl;

    target_cvr_high = 0.2;
    if (parameters.isMember("target_cvr_high")) {
        target_cvr_high = parameters["target_cvr_high"].asDouble();
    }
    cout << "target_cvr_high: " << target_cvr_high << endl;

    target_cvr_low = 0.00001;
    if (parameters.isMember("target_cvr_low")) {
        target_cvr_low = parameters["target_cvr_low"].asDouble();
    }
    cout << "target_cvr_low: " << target_cvr_low << endl;

    explore_threshold = 100;
    if (parameters.isMember("explore_threshold")) {
        explore_threshold = parameters["explore_threshold"].asInt();
    }
    cout << "explore_threshold: " << explore_threshold << endl;

    explore_base = 50;
    if (parameters.isMember("explore_base")) {
        explore_base = parameters["explore_base"].asInt();
    }
    cout << "explore_base: " << explore_base << endl;

    explore_rate = 0.25;
    if (parameters.isMember("explore_rate")) {
        explore_rate = parameters["explore_rate"].asDouble();
    }
    cout << "explore_rate: " << explore_rate << endl;

    return 0;
}

int CPXLRBidder::compute_bid(
    std::shared_ptr <BidRequest> br,
    const AdSpot &spot,
    const BidderCreative &bidCreative,
    bidder_result &result,
    bool is_purchase)
{
    return this->compute_bid_ex(br, spot, bidCreative, result, "CPC", true);
}

int CPXLRBidder::get_targets(int adid, double &target_cvr, double &target_cpa)
{
    //6 typedef DICT::reloader_t<AdCVRDict> AdCVRDict_mgr_t;
    if (_ad_cvr_dict_mgr_t == nullptr) {
        cerr << "AdCVRDict_mgr_t NULL!" << endl;
        target_cvr = this->dft_target_cvr;
        target_cpa = this->dft_target_cpa;
        return 0;
    }
    AdCVRDict *adcvr_dict = _ad_cvr_dict_mgr_t->get_content();
    if (adcvr_dict == nullptr) {
        cerr << "get adcvr_target_dict error" << endl;
        target_cvr = dft_target_cvr;
        target_cpa = dft_target_cpa;
        return 0;
    }
    double cvr = adcvr_dict->get_cvr(adid);
    if (cvr <= 0) {
        cerr << "fail to get adcvr_dict!" << endl;
        cvr = this->dft_target_cvr;
    }
    target_cvr = cvr;
    target_cpa =
        (adcvr_dict->target_cpa <= 0) ? dft_target_cpa : adcvr_dict->target_cpa;
    return 0;
}

int CPXLRBidder::compute_bid_ex(
    std::shared_ptr <BidRequest> br,
    const AdSpot &spot,
    const BidderCreative &bidCreative,
    bidder_result &result,
    const std::string &flag,
    bool is_purchase)
{
    double base_bid = float(bidCreative.ad_bid()) * bidder_rate;
    double final_ctr = result.pctr;
    //is_purchase开关打开的时候，会ctr有个cut-过滤召回效果
    if (is_purchase) {
        if (result.pctr > max_trust_ctr) {
            final_ctr = max_trust_ctr;
        } else if (result.pctr < cut_ctr) {
            final_ctr = 0.0;
        }
    }

    double ratio = bidCreative.priority_ratio();
    bool is_explore = false;
    if (bidCreative.creative_show_total() < explore_threshold
            && rand() / static_cast<double>(RAND_MAX) < explore_rate) {
        if (bidCreative.creative_show_total() == 0) {
            ratio *= explore_base;
        } else {
            ratio *= 1 + explore_base / static_cast<double>(bidCreative.creative_show_total());
        }

        is_explore = true;
    }

    double dft_target_cvr = this->dft_target_cvr;
    double dft_target_cpa = this->dft_target_cpa;

    switch (bidCreative.bid_method()) {
        case Bid_Method::BID_CPM:
        case Bid_Method::BID_CPT:
            result.price = base_bid;
            result.ratio = ratio;
            break;
        case Bid_Method::BID_CPC:
            result.price = base_bid * final_ctr;
            if (this->use_cvr && bidCreative.is_track_active() && result.pcvr > 0) {
                get_targets(bidCreative.ad_id(), dft_target_cvr, dft_target_cpa);

                float target_cvr = dft_target_cvr;
                if (use_target_cpa && bidCreative.target_cpa() > 0) {
                    target_cvr = bidCreative.ad_bid() / (1.e7 * dft_target_cpa);
                }

                if (target_cvr >= target_cvr_high || target_cvr <= target_cvr_low)
                    target_cvr = dft_target_cvr;

                double odd = result.pcvr / target_cvr;
                ratio *= pow(odd, cvr_odd_ratio);
            }
            result.ratio = ratio;
            break;
        case Bid_Method::BID_CPA:
            if (bidCreative.tracking_type() != BidMax::Tracking_Type::NOTRACKING && result.pcvr > 0) {
                result.price = base_bid * final_ctr * result.pcvr;
            } else {
                //理论上不应该走到这里!
                result.price = base_bid * final_ctr;
            }
            result.ratio = ratio;
            break;
        case Bid_Method::UNSPECIFIED:
            return -1;
        default:
            return -1;
    }

    int bid_floor = 99999;
    if (bidCreative.adx_adspot_id() == -1) {
        bid_floor = static_cast<int>(bidCreative.cpm_for_sell() * 1000);
    } else {
        bid_floor = spot.bidfloor.val;
    }

    double max_bid = bid_floor * bidCreative.max_bid_ratio();
    if (result.price > max_bid) {
        result.price = max_bid;
    }

    if (br->auctionType.val == AuctionType::FIRST_PRICE && result.price > bid_floor) {
        result.price = bid_floor * (log(result.price / bid_floor) + 1);
    }

    if (is_explore && result.price < bid_floor) {
        result.price = bid_floor;
    }

    result.priority = result.price * ratio;

    return 0;
}

int CPXLRBidder::model_reloader()
{
    if (use_cvr) {
        if (_ad_cvr_dict_mgr_t == nullptr) {
            return -1;
        }
        if (_ad_cvr_dict_mgr_t->need_reload() == 1) {
            if (_ad_cvr_dict_mgr_t->reload() < 0) {
                return -1;
            }
        }
    }
    return 0;
}

int AdCVRDict::init()
{
    return 0;
}

int AdCVRDict::load(const char *file_dir, const char *file_name)
{
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);
    std::ifstream ifs(filename);
    string line;
    while (getline(ifs, line)) {
        util::str_util::trim(line);
        transform(line.begin(), line.end(), line.begin(), ::tolower);
        vector <string> vec;
        util::str_util::split(line, " ", vec);
        if (vec.size() < 2) {
            cout << "line " << line << " format error, " << vec.size() << endl;
            continue;
        }
        int id = atoi(vec[0].c_str());
        if (id == 0 && vec.size() >= 3) {
            this->target_cpa = atof(vec[2].c_str());
        }
        float target_cvr = atof(vec[1].c_str());
        ad_cvr_dict[id] = target_cvr;
        cout << "load id:" << id << " cvr:" << target_cvr << endl;
    }
    return 0;
}

double AdCVRDict::get_cvr(int id)
{

    auto iter = ad_cvr_dict.find(id);
    if (iter != ad_cvr_dict.end()) {
        return iter->second;
    } 

    iter = ad_cvr_dict.find(0);
    if (iter != ad_cvr_dict.end()) {
        return iter->second;
    }
    return -1;
}

} /* namespace bayes */
