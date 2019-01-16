//
// Created by starsnet on 15/5/12.
//
#include "CVRMixModel.h"

namespace bayes
{
using namespace std;
using namespace BidMax;
using namespace model_dict;
CVRMixModel::CVRMixModel(int id)
{
    model_name = "CVRMixModel";
    model_id = id;
    _cvr_model_mgr_t = nullptr;
    _fmftrl_mgr_t = nullptr;
}

CVRMixModel::~CVRMixModel()
{
    if (_cvr_model_mgr_t != nullptr) {
        delete _cvr_model_mgr_t;
    }
    _cvr_model_mgr_t = nullptr;

    if (_fmftrl_mgr_t != nullptr) {
        delete _fmftrl_mgr_t;
    }
    _fmftrl_mgr_t = nullptr;

}

int CVRMixModel::init(const Json::Value &parameters)
{
    Json::Value model_dir = parameters["model_dir"];
    Json::Value model_file = parameters["model_file"];

    _cvr_model_mgr_t = new FtrlModel_mgr_t(model_dir.asCString(),
                                           model_file.asCString(), &FtrlModel_mgr_t::content_type::init);
    if (_cvr_model_mgr_t == nullptr) {
        throw ML::Exception("New cvr LR model Mgr error!");
    }
    if (_cvr_model_mgr_t->reload() < 0) {
        throw ML::Exception("cvr LR model init load error!");
    }

    this->belta_bias = 0;
    if (parameters.isMember("belta_bias")) {
        Json::Value bias_json = parameters["belta_bias"];
        this->belta_bias = bias_json.asDouble();
    }
    cout << "belta_bias: " << this->belta_bias << endl;

    using_fm = false;
    if (parameters.isMember("using_fm")) {
        Json::Value using_fm_json = parameters["using_fm"];
        using_fm = using_fm_json.asBool();
    }
    if (using_fm) {
        Json::Value model_dir = parameters["fm_dir"];
        Json::Value model_file = parameters["fm_file"];

        _fmftrl_mgr_t = new FMFtrlModel_mgr_t(model_dir.asCString(),
                                              model_file.asCString(), &FMFtrlModel_mgr_t::content_type::init);
        if (_fmftrl_mgr_t == nullptr) {
            throw ML::Exception("New FMLR model Mgr error!");
        }

        fm_bias = 0.0;
        if (parameters.isMember("fm_bias")) {
            Json::Value belta_bias_json = parameters["fm_bias"];
            fm_bias = belta_bias_json.asDouble();
        }
        cout << "fm_bias:" << fm_bias << endl;

    }

    return 0;
}

int CVRMixModel::model_reloader()
{
    if (_cvr_model_mgr_t == nullptr) {
        return -1;
    }
    if (_cvr_model_mgr_t->need_reload() == 1) {
        if (_cvr_model_mgr_t->reload() < 0) {
            return -1;
        }
    }
    if (using_fm) {
        if (_fmftrl_mgr_t == nullptr) {
            return -1;
        }
        if (_fmftrl_mgr_t->need_reload() == 1) {
            if (_fmftrl_mgr_t->reload() < 0) {
                return -1;
            }

        }
    }

    return 0;
}

double CVRMixModel::compute_cvr(
    std::shared_ptr <BidRequest> br,
    AdSpot &spot,
    const BidderCreative &bidCreative,
    const Json::Value &augmentation,
    int threadId,
    std::vector<uint32_t> & feas)
{
    auto &_ad_fields = _ad_fieldses[threadId];
    auto &_br_fields = _br_fieldses[threadId];
    auto &_fea_ad_extractor = _fea_ad_extractors[threadId];
    auto &_ad_instance = _ad_instances[threadId];
    auto &_br_instance = _br_instances[threadId];

    if (make_ad_fields(bidCreative, augmentation, _ad_fields) < 0) {
        cout << "error in make_fields" << endl;
        return -1;
    }
    _fea_ad_extractor.record_reset();

    for (size_t i = 0; i < _br_fields.size(); i++) {
        _ad_fields.push_back(_br_fields[i]);
    }

    if (!_fea_ad_extractor.add_record(_ad_fields)) {
        cout << "add record error" << endl;
        return -1;
    }
    _fea_ad_extractor.extract_fea();
    _fea_ad_extractor.get_fea_result(_ad_instance);

    string debug_fea = "new_fea: " + br->auctionId.toString() + " " +
        to_string(bidCreative.id()) + " " + spot.banner->id.toString() +
        " " + br->deviceString;

    debug_fea += " br:";
    for (size_t i = 0; i < _br_instance.fea_vec.size(); i++) {
        debug_fea += to_string(_br_instance.fea_vec[i]) + " ";
    }
    debug_fea += " ad:";
    for (size_t i = 0; i < _ad_instance.fea_vec.size(); i++) {
        debug_fea += to_string(_ad_instance.fea_vec[i]) + " ";
    }
    _ad_instance.add(_br_instance);

    // fea log
    feas = _ad_instance.fea_vec;

    FtrlModel *ftrl_model = _cvr_model_mgr_t->get_content();
    if (ftrl_model == nullptr) {
        cerr << "get ftrl error" << endl;
        return -1;
    }

    double pcvr = ftrl_model->score(_ad_instance, belta_bias);
    if (using_fm) {
        FMFtrlModel *fmftrl_model = _fmftrl_mgr_t->get_content();
        if (fmftrl_model == nullptr) {
            cerr << "get fmftrl error" << endl;
            return -1;
        }
        pcvr = fmftrl_model->score(_ad_instance, fm_bias);
    }

    return pcvr;
}

} /* namespace bayes */
