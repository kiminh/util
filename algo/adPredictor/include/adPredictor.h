/*************************************************************************
    > File Name: adPredictor.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 12/24 23:35:05 2014
 ************************************************************************/
#ifndef AD_PREDICTOR_H
#define AD_PREDICTOR_H

#include <string>
#include "sparse_fea.h"
#include "str_util.h"

namespace algo {
    struct InstanceStatInfo {
        double ins_fea_mea;
        double ins_fea_var;
    };

    struct VW {
        double v;
        double w;
    };

    class adPredictor {
    public:
        adPredictor(int fea_num, double beta);

        adPredictor();

    public:
        void train_line(std::string &line);

        void train_instance(fea::instance &ins);

        bool save_model(const std::string &model_file);

    public:
        double predict(std::string &line);

        double predict_instance(fea::instance &ins);

        bool load_model(const std::string &file_name);

    public:
        void free_model();

        bool parse_line(std::string &line, fea::instance &ins);

    private:
        void computeVW(double factor, VW &vm);

        void compute_mean_var(InstanceStatInfo &statInfo,
                              fea::fea_list &ins_fea_list);

        double cdf_norm(double z);

    private:
        int fea_num;
        double *fea_mea; //
        double *fea_var; //
        double beta;
        static const double PI;
        VW vw;
        InstanceStatInfo statInfo;

    private:
        util::str_util m_str_util;
    };
}
#endif
