/*
 * ShowModel.cpp
 *
 *  Created on: 2015年3月4日
 *      Author: starsnet
 */

#include "ShowModel.h"

namespace bayes
{
using namespace std;
using namespace BidMax;

ShowModel::ShowModel(int id)
{
    model_name = "ShowModel";
    model_id = id;
    _show_mgr_t = nullptr;
}

ShowModel::~ShowModel()
{
    if (_show_mgr_t != nullptr) {
        delete _show_mgr_t;
    }
    _show_mgr_t = nullptr;
}

int ShowModel::init(const Json::Value &parameters)
{
    Json::Value model_dir = parameters["model_dir"];
    Json::Value model_file = parameters["model_file"];

    _show_mgr_t = new ShowDict_mgr_t(model_dir.asCString(),
                                     model_file.asCString(), &ShowDict_mgr_t::content_type::init);
    if (_show_mgr_t == nullptr) {
        throw ML::Exception("New show model Mgr error!");
    }

    Json::Value max_show_json = parameters["max_show"];
    max_show = max_show_json.asInt();
    cout << "max show " << max_show << endl;

    return 0;
}

int ShowModel::model_reloader()
{
    if (_show_mgr_t == nullptr) {
        return -1;
    }
    if (_show_mgr_t->need_reload() == 1) {
        if (_show_mgr_t->reload() < 0) {
            return -1;
        }
    }
    return 0;
}

int ShowModel::compute_fea(
    std::shared_ptr <BidRequest> br,
    AdSpot &spot,
    const BidderCreative &bidCreative,
    string &account,
    std::string &result)
{
    //make fields
    string user_creative;

    if (make_fields(br, spot, bidCreative, account, user_creative) < 0) {
        cout << "error in make_fields" << endl;
        return -1;
    }
    ShowDict *p_showdict = _show_mgr_t->get_content();
    if (p_showdict == nullptr) {
        cerr << "get show dict init error" << endl;
        return -1;
    }

    //int show = 0;
    //int clk = 0;
    return p_showdict->extract_showclk(user_creative, result);
}

int ShowModel::make_fields(
    shared_ptr <BidRequest> br,
    AdSpot &spot,
    const BidderCreative &bidCreative,
    string &account,
    string &info)
{
    //app or site info only one is exist
    if ((br->site && br->app) || (!br->site && !br->app)) {
        return -1;
    }

    string device("");
    if (br->device) {
        if (!br->idfa.empty() && br->device->os.rawString() == "iOS") {
            device = br->idfa;
        } else {
            if (!(br->device->didsha1).empty()) {
                device = br->device->didsha1;
            } else if (!(br->device->didmd5).empty()) {
                device = br->device->didmd5;
            } else if (!(br->device->dpidsha1).empty()) {
                device = br->device->dpidsha1;
            } else {
                device = br->device->dpidmd5;
            }
        }

    }

    info = device + "_" + to_string(bidCreative.id());

    return 0;
}

int ShowDict::init()
{
    return 0;
}

int ShowDict::load(const char *file_dir, const char *file_name)
{
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);

    std::ifstream model_file(filename);

    string line;
    //cout << "show thr " << show_thr << endl;
    while (getline(model_file, line)) {
        util::str_util::trim(line);
        vector <string> vec;
        util::str_util::split(line, "\t", vec);
        if (vec.size() < 3) {
            cout << "line " << line << " format error" << endl;
            continue;
        }

        if (vec[0].empty() || vec[1].empty()) {
            continue;
        }
        string user_creative = vec[0];
        transform(user_creative.begin(), user_creative.end(), user_creative.begin(), ::tolower);
        //if(show < show_thr){
        //	continue;
        //}
        //cout << "add into " << app_creative << " " << show << endl;
        showclk_dict[user_creative] = vec[1] + "_" + vec[2];
    }

    return 0;
}

int ShowDict::extract_showclk(string &key, string &fea_res)
{
    fea_res.clear();
    transform(key.begin(), key.end(), key.begin(), ::tolower);
    unordered_map<string, string>::iterator iter = showclk_dict.find(key);
    if (iter != showclk_dict.end()) {
        fea_res = iter->second;
    }
    return 0;
}
} /* namespace bayes */
