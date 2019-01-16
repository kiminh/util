#include "CTRLRKVModel.h"

namespace bayes
{
using namespace std;
using namespace BidMax;
using namespace model_dict;

CTRLRKVModel::CTRLRKVModel(int id)
    : CTRModelBase("CTRLRKVModel", id)
    , _ftrl_kv_model_mgr_t(nullptr)
    , using_gbdt(false)
    , _gbdt_model_mgr_t(nullptr)
{
}

CTRLRKVModel::~CTRLRKVModel()
{
    if (_ftrl_kv_model_mgr_t != nullptr) {
        delete _ftrl_kv_model_mgr_t;
    }
    _ftrl_kv_model_mgr_t = nullptr;

    if (using_gbdt) {
        if (_gbdt_model_mgr_t != nullptr) {
            delete _gbdt_model_mgr_t;
        }
        _gbdt_model_mgr_t = nullptr;
    }
}

int CTRLRKVModel::init(const Json::Value &parameters)
{
    CTRModelBase::init(parameters);

    Json::Value model_dir = parameters["model_dir"];
    Json::Value model_file = parameters["model_file"];
    _ftrl_kv_model_mgr_t = new FtrlKVModel_mgr_t(model_dir.asCString(),
                                            model_file.asCString(), &FtrlKVModel_mgr_t::content_type::init);
    if (_ftrl_kv_model_mgr_t == nullptr) {
        throw ML::Exception("New LR model Mgr error!");
    }

    if (parameters.isMember("using_gbdt")) {
        using_gbdt = parameters["using_gbdt"].asBool();
    }

    if (using_gbdt) {
        Json::Value model_dir = parameters["gbdt_model_dir"];
        Json::Value model_file = parameters["gbdt_model_file"];
        cout << "gbdt_model_dir: " << model_dir.asCString() << " gbdt_model_file: " << model_file.asCString() << endl;
        _gbdt_model_mgr_t = new GBDTModel_mgr_t(model_dir.asCString(),
                                                model_file.asCString(), &GBDTModel_mgr_t::content_type::init);
        if (_gbdt_model_mgr_t == nullptr) {
            throw ML::Exception("New GBDT model Mgr error!");
        }

        gbdt_hash_bias = parameters["gbdt_hash_bias"].asUInt();
        cout << "gbdt has bias: " << gbdt_hash_bias << endl;
    }
    return 0;
}

int CTRLRKVModel::model_reloader()
{
    if (_ftrl_kv_model_mgr_t == nullptr) {
        return -1;
    }
    if (_ftrl_kv_model_mgr_t->need_reload() == 1) {
        if (_ftrl_kv_model_mgr_t->reload() < 0) {
            return -1;
        }
    }

    if (using_gbdt) {
        if (_gbdt_model_mgr_t == nullptr) {
            return -1;
        }
        if (_gbdt_model_mgr_t->need_reload() == 1) {
            if (_gbdt_model_mgr_t->reload() < 0) {
                return -1;
            }
        }
    }

    return 0;
}

double CTRLRKVModel::compute_ctr(
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
    //fprintf(stdout, "%s\n", debug_fea.c_str());
    _ad_instance.add(_br_instance);

    // fea log
    feas = _ad_instance.fea_vec;

    if (_ftrl_kv_model_mgr_t == nullptr) {
        cerr << "_ftrl_model_mgr_t null" << endl;
        return 0.0;
    }

    FtrlKVModel *ftrl_kv_model = _ftrl_kv_model_mgr_t->get_content();
    if (ftrl_kv_model == nullptr) {
        cerr << "get ftrl error" << endl;
        return 0.0;
    }
    float pctr = ftrl_kv_model->score(_ad_instance, belta_bias);
    //cout << "ftrl pctr: " << pctr << endl;

    if (using_gbdt) {
        if (_gbdt_model_mgr_t == nullptr) {
            cerr << "_gbdt_model_mgr_t  error" << endl;
            return pctr;
        }
        GBDTModel *gbdt_model = _gbdt_model_mgr_t->get_content();

        if (gbdt_model != nullptr) {
            vector <uint32_t> gbdt_fea;
            cout << "before join: ad_isntance: " << _ad_instance.fea_vec.size() << endl;
            gbdt_model->join_fea(_ad_instance, gbdt_fea, gbdt_hash_bias);
            cout << "after join: ad_isntance: " << _ad_instance.fea_vec.size() << endl;
            pctr = ftrl_kv_model->score(_ad_instance, belta_bias);
        }
    }

    return pctr;
}

} /* namespace bayes */
