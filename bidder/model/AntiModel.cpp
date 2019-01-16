/*
 * AntiModel.cpp
 *
 *  Created on: 2015年3月4日
 *      Author: starsnet
 */

#include "AntiModel.h"

namespace bayes
{
using namespace std;
using namespace BidMax;

AntiModel::AntiModel(int id)
{
    model_name = "AntiModel";
    model_id = id;
    _anti_mgr_t = nullptr;
}

AntiModel::~AntiModel()
{
    if (_anti_mgr_t != nullptr) {
        delete _anti_mgr_t;
    }
    _anti_mgr_t = nullptr;
}

int AntiModel::init(const Json::Value &parameters)
{

    Json::Value model_dir = parameters["model_dir"];
    Json::Value model_file = parameters["model_file"];

    _anti_mgr_t = new AntiDict_mgr_t(model_dir.asCString(),
                                     model_file.asCString(),
                                     &AntiDict_mgr_t::content_type::init);
    if (_anti_mgr_t == nullptr) {
        throw ML::Exception("New anti model Mgr error!");
    }
    //bool enable_newuser_filter;
    this->enable_newuser_filter = false;
    if (parameters.isMember("enable_newuser_filter")) {
        this->enable_newuser_filter = parameters["enable_newuser_filter"].asBool();
    }
    cout << "anti_model: enabel_newuser_filter: " << this->enable_newuser_filter << endl;

    return 0;
}

int AntiModel::model_reloader()
{
    if (_anti_mgr_t == nullptr) {
        return -1;
    }
    if (_anti_mgr_t->need_reload() == 1) {
        if (_anti_mgr_t->reload() < 0) {
            return -1;
        }
    }
    return 0;
}

bool AntiModel::is_medium_spam(string app)
{
    AntiDict *p_antidict = _anti_mgr_t->get_content();
    if (p_antidict == nullptr) {
        cerr << "get anti dict init error" << endl;
        return false;
    }
    return p_antidict->is_medium_spam(app);
}

bool AntiModel::is_user_spam(string user, int id)
{
    AntiDict *p_antidict = _anti_mgr_t->get_content();
    if (p_antidict == nullptr) {
        cerr << "get anti dict init error" << endl;
        return false;
    }
    return p_antidict->is_user_spam(user, id);
}

bool AntiModel::is_user_new(string user)
{
    if (!enable_newuser_filter) {
        return false;
    }
    AntiDict *p_antidict = _anti_mgr_t->get_content();
    if (p_antidict == nullptr) {
        cerr << "get anti dict init error" << endl;
        return false;
    }
    return p_antidict->is_user_new(user);
}

int AntiDict::init()
{
    return 0;
}

int AntiDict::load(const char *file_dir, const char *file_name)
{
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);

    std::ifstream model_file(filename);

    string line;
    //cout << "anti thr " << anti_thr << endl;
    while (getline(model_file, line)) {
        transform(line.begin(), line.end(), line.begin(), ::tolower);
        util::str_util::trim(line);
        vector <string> vec;
        util::str_util::split(line, "\t", vec);
        if (vec.size() < 2) {
            cout << "line " << line << " format error" << endl;
            continue;
        }
        int tag = atoi(vec[1].c_str());
        if (tag == APP_TAG) {
            spam_app_set.insert(vec[0]);
        } else if (tag == USER_TAG) {
            spam_user_set.insert(vec[0]);
        } else if (tag == NEW_TAG) {
            old_user_set.insert(vec[0]);
        } else {
            cerr << "tag " << vec[1] << " not verified" << endl;
        }
    }

    return 0;
}

bool AntiDict::is_medium_spam(string app)
{
    transform(app.begin(), app.end(), app.begin(), ::tolower);
    unordered_set<string>::iterator iter = spam_app_set.find(app);
    if (iter != spam_app_set.end()) {
        return true;
    }
    return false;
}

bool AntiDict::is_user_spam(string user, int id)
{
    char tmp[256];
    snprintf(tmp, 256, "%s_%d", user.c_str(), id);
    string key(tmp);
    transform(key.begin(), key.end(), key.begin(), ::tolower);
    unordered_set<string>::iterator iter = spam_user_set.find(key);
    if (iter != spam_user_set.end()) {
        return true;
    }
    return false;
}

bool AntiDict::is_user_new(string user)
{
    transform(user.begin(), user.end(), user.begin(), ::tolower);
    unordered_set<string>::iterator iter = old_user_set.find(user);
    //not in old user,so it is new
    if (iter == old_user_set.end()) {
        return true;
    }
    return false;
}

} /* namespace bayes */
