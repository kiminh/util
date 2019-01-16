#include "CVRGBDTModel.h"

namespace bayes
{
using namespace std;
using namespace BidMax;
using namespace model_dict;

CVRGBDTModel::CVRGBDTModel(int id)
    : CVRModelBase("CVRGBDTModel", id)
    , _gbdt_model_mgr_t(nullptr)
{
}

CVRGBDTModel::~CVRGBDTModel()
{
    if (_gbdt_model_mgr_t != nullptr) {
        delete _gbdt_model_mgr_t;
    }
    _gbdt_model_mgr_t = nullptr;
}

int CVRGBDTModel::init(const Json::Value &parameters)
{
    CVRModelBase::init(parameters);

    Json::Value model_dir = parameters["model_dir"];
    Json::Value model_file = parameters["model_file"];
    cout << "model_dir: " << model_dir.asCString() << " model_file: " << model_file.asCString() << endl;
    _gbdt_model_mgr_t = new GBDTModel_mgr_t(model_dir.asCString(),
                                            model_file.asCString(), &GBDTModel_mgr_t::content_type::init);
    if (_gbdt_model_mgr_t == nullptr) {
        throw ML::Exception("New GBDT model Mgr error!");
    }
    
    return 0;
}

int CVRGBDTModel::model_reloader()
{
    if (_gbdt_model_mgr_t == nullptr) {
        return -1;
    }
    if (_gbdt_model_mgr_t->need_reload() == 1) {
        if (_gbdt_model_mgr_t->reload() < 0) {
            return -1;
        }
    }

    return 0;
}

double CVRGBDTModel::compute_cvr(
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

    double pcvr = 0.0;
    if (_gbdt_model_mgr_t == nullptr) {
        cerr << "_gbdt_model_mgr_t  error" << endl;
        return pcvr;
    }
    GBDTModel *gbdt_model = _gbdt_model_mgr_t->get_content();

    if (gbdt_model != nullptr) {
        pcvr = gbdt_model->score(_ad_instance);
//        cout << "get pcvr: " << pcvr << endl;
    }


    return pcvr;
}

} /* namespace bayes */
