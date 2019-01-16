/*
 * CTRModelBase.cpp
 *
 *  Created on: 2015年3月1日
 *      Author: starsnet
 */

#include "CTRModelBase.h"

namespace bayes
{
using namespace std;

CTRModelBase::CTRModelBase()
{
    for (int i = 0; i < threadNum; ++i) {
        _fea_extractors.push_back(fea::extractor());
        _fea_br_extractors.push_back(fea::extractor());
        _fea_ad_extractors.push_back(fea::extractor());

        _br_instances.push_back(fea::instance());
        _ad_instances.push_back(fea::instance());
        _instances.push_back(fea::instance());

        _br_fieldses.push_back(vector<string>());
        _ad_fieldses.push_back(vector<string>());
    }
}

CTRModelBase::CTRModelBase(string name, int id)
    : model_name(name)
    , model_id(id)
{
    for (int i = 0; i < threadNum; ++i) {
        _fea_extractors.push_back(fea::extractor());
        _fea_br_extractors.push_back(fea::extractor());
        _fea_ad_extractors.push_back(fea::extractor());

        _br_instances.push_back(fea::instance());
        _ad_instances.push_back(fea::instance());
        _instances.push_back(fea::instance());

        _br_fieldses.push_back(vector<string>());
        _ad_fieldses.push_back(vector<string>());
    }
}

CTRModelBase::~CTRModelBase()
{
    // TODO Auto-generated destructor stub
}

const string &CTRModelBase::get_model_name()
{
    return model_name;
}

int CTRModelBase::init(const Json::Value &parameters)
{

    Json::Value ad_fea_br_conf = parameters["ad_fea_br"];
    for (int i = 0; i < threadNum; ++i) {
        if (!_fea_br_extractors[i].init(ad_fea_br_conf.asCString())) {
            return -1;
        }
    }

    Json::Value ad_fea_ad_conf = parameters["ad_fea_ad"];
    for (int i = 0; i < threadNum; ++i) {
        if (!_fea_ad_extractors[i].init(ad_fea_ad_conf.asCString())) {
            return -1;
        }
    }

    belta_bias = 0.0;
    if (parameters.isMember("belta_bias")) {
        Json::Value belta_bias_json = parameters["belta_bias"];
        belta_bias = belta_bias_json.asDouble();
    }
    cout << "belta_bias:" << belta_bias << endl;

    return 0;

}

double CTRModelBase::compute_ctr(
    std::shared_ptr <BidRequest> br,
    AdSpot &spot,
    const BidderCreative &bidCreative,
    const Json::Value &augmentation,
    int threadId,
    std::vector<uint32_t> & feas)
{
    return 0;
}

int CTRModelBase::make_br_fields(shared_ptr <BidRequest> br, AdSpot &spot, vector <string> &extract_fields)
{
    //clear fields
    //extract_fields
    extract_fields.clear();
    extract_fields.push_back(br->exchange);        //exchange
    extract_fields.push_back(br->provider);        //provider
    extract_fields.push_back(br->userAgentIPHash.toString());        //id
    //app or site info only one is exist
    if ((br->site && br->app) || (!br->site && !br->app)) {
        return -1;
    }

    //for site and app
    if (br->site) {
        extract_fields.push_back(br->site->id.toString());
        extract_fields.push_back(br->site->name.rawString());
        extract_fields.push_back(br->site->domain.rawString());
        if (br->site->cat.size() > 0) {
            std::string cats(br->site->cat[0].val);
            for (size_t i = 1; i < br->site->cat.size(); ++i) {
                cats.append("_").append(br->site->cat[i].val);
            }
            extract_fields.push_back(cats);
        } else {
            extract_fields.push_back("");
        }
    } else {
        extract_fields.push_back(br->app->id.toString());
        extract_fields.push_back(br->app->name.rawString());
        extract_fields.push_back(br->app->domain.rawString());
        if (br->app->cat.size() > 0) {
            std::string cats(br->app->cat[0].val);
            for (size_t i = 1; i < br->app->cat.size(); ++i) {
                cats.append("_").append(br->app->cat[i].val);
            }
            extract_fields.push_back(cats);
        } else {
            extract_fields.push_back("");
        }
    }
    string user_id("");
    if (br->device) {
        //      extract_fields.push_back(br->device->ua.rawString());
        //      extract_fields.push_back(br->device->ip);
        if (!br->idfa.empty() && br->device->os.rawString() == "iOS") {
            user_id = br->idfa;
            extract_fields.push_back(br->idfa);//1
            extract_fields.push_back(br->idfa);//1
        } else {
            user_id = br->device->didsha1;
            extract_fields.push_back(br->device->didsha1);//1
            extract_fields.push_back(br->device->didmd5);//2
        }
        extract_fields.push_back(br->device->dpidsha1);//3
        extract_fields.push_back(br->device->dpidmd5);//4
        extract_fields.push_back(br->device->carrier.rawString());//5
        //      extract_fields.push_back(br->device->language.rawString());//6
        extract_fields.push_back(br->device->make.rawString());//7
        extract_fields.push_back(br->device->model.rawString());//8
        extract_fields.push_back(br->device->os.rawString());//9
        extract_fields.push_back(br->device->osv.rawString());//10
        extract_fields.push_back(to_string(br->device->devicetype.value()));//11
        extract_fields.push_back(to_string(br->device->connectiontype.value()));//12
    } else {
        extract_fields.push_back("");//1
        extract_fields.push_back("");//2
        extract_fields.push_back("");//3
        extract_fields.push_back("");//4
        extract_fields.push_back("");//5
        //      extract_fields.push_back("");//6
        extract_fields.push_back("");//7
        extract_fields.push_back("");//8
        extract_fields.push_back("");//9
        extract_fields.push_back("");//10
        extract_fields.push_back("");//11
        extract_fields.push_back("");//12
    }
    //language
    extract_fields.push_back(br->language.rawString());
    //Location location;      ///< Best available location information
    extract_fields.push_back(br->location.fullLocationString().rawString());
    //url
    extract_fields.push_back(br->url.original);
    //std::string ipAddress;
    extract_fields.push_back(br->ipAddress);
    extract_fields.push_back(br->userAgent.rawString());
    //    Date timestamp;
    extract_fields.push_back(to_string(br->timestamp.hour()));
    extract_fields.push_back(to_string(br->timestamp.weekday()));
    // AdSpot& spot,
    //spot.banner->id
    //extract_fields.push_back(spot.id.toString());
    extract_fields.push_back(spot.banner->id.toString());

    extract_fields.push_back(br->device->geo->country.rawString());
    extract_fields.push_back(br->device->geo->region.rawString());
    extract_fields.push_back(br->device->geo->city.rawString());
    extract_fields.push_back(br->device->geo->zip.rawString());
    extract_fields.push_back(br->device->geo->metro.rawString());

    if (br->site) {
        extract_fields.push_back("1");
    } else {
        extract_fields.push_back("0");
    }

    extract_fields.push_back(br->deviceString);

    if (br->site && !br->site->page.empty()) {
        cout << "add site->page: " << br->site->page.original << endl;
        extract_fields.push_back(br->site->page.original);
    } else if (br->app && !br->app->bundle.empty()) {
        cout << "add app->bundle: " << br->app->bundle.rawString() << endl;
        extract_fields.push_back(br->app->bundle.rawString());
    } else {
        extract_fields.push_back("");
    }
    return 0;
}

int CTRModelBase::make_ad_fields(
    const BidderCreative &bidCreative,
    const Json::Value &augmentation,
    vector <string> &extract_fields)
{
    //Creative& bidCreative,
    extract_fields.clear();

    extract_fields.push_back(to_string(bidCreative.id()));
    extract_fields.push_back(to_string(bidCreative.ads_user_id()));
    extract_fields.push_back(bidCreative.cat_level1());
    extract_fields.push_back(bidCreative.cat_level2());
    extract_fields.push_back(bidCreative.format());
    extract_fields.push_back(to_string(bidCreative.ad_type()));
    extract_fields.push_back(to_string(bidCreative.native_type()));
    extract_fields.push_back(to_string(bidCreative.ad_id()));

    // creative_freq and aduser_freq feature
    int creative_freq = 0;
    int aduser_freq = 0;
    if (augmentation.isMember("data")) {
        Json::Value data = augmentation["data"];
        try {
            Json::Value freq_data;
            freq_data["total"] = data["total"].asUInt();
            if (data["cid"].isArray() && data["freq_data"].isArray()) {
                for (Json::Value::UInt i = 0; i < data["cid"].size(); ++i) {
                    //cout << "cid: " << data["cid"][i].asInt() << endl;
                    int cid = data["cid"][i].asInt();
                    if (cid == bidCreative.id()) {
                        creative_freq = data["freq_data"][i]["cid"].asInt();
                        aduser_freq = data["freq_data"][i]["ad_id"].asInt();
                    }
                }
            }
        } catch (...) {
            cerr << "Parse byc-augmentor json data failed" << endl;
            //FATAL_LOG("Parse byc-augmentor json data failed.");
        }
    }
    extract_fields.push_back(to_string(creative_freq));
    extract_fields.push_back(to_string(aduser_freq));

    //cout << "creat_freq: " << creative_freq << " aduser_freq: " << aduser_freq << endl;

    if (!bidCreative.has_native_info()) {
        // to make features even with before
        // maybe this is not needed
        extract_fields.push_back("");
        extract_fields.push_back("");
        return 0;
    }

    // extract native info
    auto &native_info = bidCreative.native_info();
    string image_fea = "";
    string word_fea = "";

    for (int i = 0; i < native_info.images_size(); i++) {
        auto &image_info = native_info.images(i);
        image_fea += "\002" + to_string(image_info.image_type()) + "_" +
            to_string(image_info.width()) + "_" + to_string(image_info.height());
    }

    if (!image_fea.empty()) {
        image_fea = image_fea.substr(1);
    }

    for (int j = 0; j < native_info.words_size(); j++) {
        auto &word_info = native_info.words(j);
        word_fea += "\002" + to_string(word_info.word_type()) + "_" +
            to_string(word_info.length());
    }

    if (!word_fea.empty()) {
        word_fea = word_fea.substr(1);
    }

    extract_fields.push_back(image_fea);
    extract_fields.push_back(word_fea);

    return 0;
}

int CTRModelBase::make_fields(
    shared_ptr <BidRequest> br,
    AdSpot &spot,
    const BidderCreative &bidCreative,
    vector <string> &extract_fields)
{
    extract_fields.push_back(br->exchange);
    extract_fields.push_back(br->provider);
    extract_fields.push_back(br->userAgentIPHash.toString());

    //app or site info only one is exist
    if ((br->site && br->app) || (!br->site && !br->app)) {
        return -1;
    }

    //for site and app
    if (br->site) {
        extract_fields.push_back(br->site->id.toString());
        extract_fields.push_back(br->site->name.rawString());
        extract_fields.push_back(br->site->domain.rawString());
        if (br->site->cat.size() > 0) {
            std::string cats(br->site->cat[0].val);
            for (size_t i = 1; i < br->site->cat.size(); ++i) {
                cats.append("_").append(br->site->cat[i].val);
            }
            extract_fields.push_back(cats);
        } else {
            extract_fields.push_back("");
        }
    } else {
        extract_fields.push_back(br->app->id.toString());
        extract_fields.push_back(br->app->name.rawString());
        extract_fields.push_back(br->app->domain.rawString());
        if (br->app->cat.size() > 0) {
            std::string cats(br->app->cat[0].val);
            for (size_t i = 1; i < br->app->cat.size(); ++i) {
                cats.append("_").append(br->app->cat[i].val);
            }
            extract_fields.push_back(cats);
        } else {
            extract_fields.push_back("");
        }
    }
    string user_id("");
    if (br->device) {
        //      extract_fields.push_back(br->device->ua.rawString());
        //      extract_fields.push_back(br->device->ip);
        if (!br->idfa.empty() && br->device->os.rawString() == "iOS") {
            user_id = br->idfa;
            extract_fields.push_back(br->idfa);//1
            extract_fields.push_back(br->idfa);//1
        } else {
            user_id = br->device->didsha1;
            extract_fields.push_back(br->device->didsha1);//1
            extract_fields.push_back(br->device->didmd5);//2
        }
        extract_fields.push_back(br->device->dpidsha1);//3
        extract_fields.push_back(br->device->dpidmd5);//4
        extract_fields.push_back(br->device->carrier.rawString());//5
        //      extract_fields.push_back(br->device->language.rawString());//6
        extract_fields.push_back(br->device->make.rawString());//7
        extract_fields.push_back(br->device->model.rawString());//8
        extract_fields.push_back(br->device->os.rawString());//9
        extract_fields.push_back(br->device->osv.rawString());//10
        extract_fields.push_back(to_string(br->device->devicetype.value()));//11
        extract_fields.push_back(to_string(br->device->connectiontype.value()));//12
    } else {
        extract_fields.push_back("");//1
        extract_fields.push_back("");//2
        extract_fields.push_back("");//3
        extract_fields.push_back("");//4
        extract_fields.push_back("");//5
        //      extract_fields.push_back("");//6
        extract_fields.push_back("");//7
        extract_fields.push_back("");//8
        extract_fields.push_back("");//9
        extract_fields.push_back("");//10
        extract_fields.push_back("");//11
        extract_fields.push_back("");//12
    }
    //language
    extract_fields.push_back(br->language.rawString());
    //Location location;      ///< Best available location information
    extract_fields.push_back(br->location.fullLocationString().rawString());
    //url
    extract_fields.push_back(br->url.original);
    //std::string ipAddress;
    extract_fields.push_back(br->ipAddress);
    //userAgent
    extract_fields.push_back(br->userAgent.rawString());
    //    Date timestamp;
    extract_fields.push_back(to_string(br->timestamp.hour()));
    extract_fields.push_back(to_string(br->timestamp.weekday()));
    // AdSpot& spot,
    //spot.banner->id
    //extract_fields.push_back(spot.id.toString());
    extract_fields.push_back(spot.banner->id.toString());

    //Creative& bidCreative,
    extract_fields.push_back(to_string(bidCreative.id()));
    extract_fields.push_back(to_string(bidCreative.ads_user_id()));
    extract_fields.push_back(bidCreative.cat_level1());
    extract_fields.push_back(bidCreative.cat_level2());
    extract_fields.push_back(bidCreative.format());
    extract_fields.push_back(to_string(bidCreative.ad_type()));

    extract_fields.push_back(br->device->geo->country.rawString());
    extract_fields.push_back(br->device->geo->region.rawString());
    extract_fields.push_back(br->device->geo->city.rawString());
    extract_fields.push_back(br->device->geo->zip.rawString());
    extract_fields.push_back(br->device->geo->metro.rawString());

    //TODO
    if (user_id.size() > 100) {
        extract_fields.push_back("1");
    } else {
        extract_fields.push_back("0");
    }

    return 0;
}

int CTRModelBase::model_reloader()
{
    return 0;
}

int CTRModelBase::extract_br(std::shared_ptr <BidRequest> br, AdSpot &spot, int threadId)
{
    auto &_br_fields = _br_fieldses[threadId];
    auto &_fea_br_extractor = _fea_br_extractors[threadId];
    auto &_br_instance = _br_instances[threadId];

    if (make_br_fields(br, spot, _br_fields) < 0) {
        cout << "error in make_fields" << endl;
        return -1;
    }
    _fea_br_extractor.record_reset();
    if (!_fea_br_extractor.add_record(_br_fields)) {
        cout << "ctr_model_base: add record error" << endl;
        return -1;
    }
    _fea_br_extractor.extract_fea();
    _fea_br_extractor.get_fea_result(_br_instance);
    return 0;
}

} /* namespace bayes */
