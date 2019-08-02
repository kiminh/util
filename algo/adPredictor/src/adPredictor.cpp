/*************************************************************************
    > File Name: adPredictor.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 12/24 23:35:05 2014
 ************************************************************************/
#include <cmath>
#include <fstream>
#include "adPredictor.h"

namespace algo {
    using namespace std;

    const double adPredictor::PI = 3.1415926535;

    adPredictor::adPredictor(int fea_num, double beta) {
        this->fea_num = fea_num;
        this->beta = beta;
        this->fea_mea = new double[fea_num];
        this->fea_var = new double[fea_num];
        for (int index = 0; index < fea_num; ++index) {
            fea_mea[index] = 0.0;
            fea_var[index] = 1.0;
        }
    }

    adPredictor::adPredictor() {
        this->fea_num = 0;
        this->beta = 1;
    }

    void adPredictor::train_line(string & line) {
        fea::instance instance;
        if (parse_line(line, instance)) {
            train_instance(instance);
        }
    }

    void adPredictor::train_instance(fea::instance & ins) {
        fea::fea_list &ins_feas = ins.fea_vec;

        compute_mean_var(statInfo, ins_feas);
        double team_factor = ins.label * statInfo.ins_fea_mea
                             / sqrt(statInfo.ins_fea_var);
        computeVW(team_factor, vw);

        for (size_t index = 0; index < ins_feas.size(); ++index) {
            int fea_index = ins_feas[index].fea_index;
            if (fea_index >= fea_num)
                continue;
            fea_mea[fea_index] += ins.label * fea_var[fea_index] * vw.v /
                                  sqrt(statInfo.ins_fea_var);
            fea_var[fea_index] *= (1 - fea_var[fea_index]
                                       / statInfo.ins_fea_var * vw.w);
        }
    }

    double adPredictor::predict(string & line) {
        fea::instance ins;
        parse_line(line, ins);
        return predict_instance(ins);
    }

    double adPredictor::predict_instance(fea::instance & ins) {
        compute_mean_var(statInfo, ins.fea_vec);
        return cdf_norm(statInfo.ins_fea_mea / sqrt(statInfo.ins_fea_var));
    }

    bool adPredictor::parse_line(std::string & line, fea::instance & ins) {
        vector <string> fields;
        util::str_util::split(line, " ", fields);
        ins.label = m_str_util.castFromS<int>(fields[0]) == 1 ? 1 : -1;

        vector <string> fea_info;
        for (size_t index = 1; index < fields.size(); ++index) {
            fea::fea_item item;
            fea_info.clear();

            util::str_util::split(fields[index], ":", fea_info);
            item.fea_index = m_str_util.castFromS<int>(fea_info[0]);
            item.fea_value = 1;
            ins.fea_vec.push_back(item);
        }
        return true;
    }

    bool adPredictor::save_model(const string &file_name) {
        ofstream modelWriter(file_name);

        modelWriter << fea_num << endl;
        modelWriter << beta << endl;
        for (int index = 0; index < fea_num; ++index) {
            modelWriter << index << "\t" << fea_mea[index] << "\t" << fea_var[index] << endl;
        }
        modelWriter.close();
        return true;
    }

    bool adPredictor::load_model(const string &file_name) {
        ifstream modelReader(file_name);
        free_model();

        modelReader >> fea_num;
        fea_mea = new double[fea_num];
        fea_var = new double[fea_num];

        modelReader >> beta;
        int index = 0;
        double mean = 0.0;
        double var = 0.0;

        while (modelReader >> index >> mean >> var) {
            ++index;
            fea_mea[index] = mean;
            fea_var[index] = var;
        }

        modelReader.close();
        if (index != fea_num) {
            free_model();
            return false;
        }
        return true;
    }

    void adPredictor::free_model() {
        if (fea_mea != NULL) {
            delete[] fea_mea;
            fea_mea = NULL;
        }
        if (fea_var != NULL) {
            delete[] fea_var;
            fea_var = NULL;
        }
    }

    void adPredictor::computeVW(double factor, VW &vm) {
        if (factor >= 20) {
            vm.v = 0.0;
            vm.w = 0.0;
        }
        else if (factor > -7) {
            double gaussian = exp((-0.5) * factor * factor)
                              / sqrt(2 * PI);
            double cdf = cdf_norm(factor);
            vm.v = gaussian / cdf;
            vm.w = vm.v * (vm.v + factor);
        }
        else {
            static double a = 0.339;
            static double b = 5.51;
            vm.v = a * sqrt(factor * factor + b) - (1 - a) * factor;
            vm.w = vm.v * (vm.v + factor);
        }
    }

    void adPredictor::compute_mean_var(InstanceStatInfo & statInfo,
                                       fea::fea_list & ins_fea_list) {
        statInfo.ins_fea_mea = 0;
        statInfo.ins_fea_var = 0;
        for (size_t index = 0; index < ins_fea_list.size(); ++index) {
            int fea_index = ins_fea_list[index].fea_index;

            if (fea_index >= fea_num) {
                continue;
            }
            statInfo.ins_fea_mea += fea_mea[fea_index];
            statInfo.ins_fea_var += fea_var[fea_index];
        }
        statInfo.ins_fea_var += beta * beta;
    }

    double adPredictor::cdf_norm(double z) {
        static double a1 = 0.254829592;
        static double a2 = -0.284496736;
        static double a3 = 1.421413741;
        static double a4 = -1.453152027;
        static double a5 = 1.061405429;
        static double p = 0.3275911;

        int sign = 1;

        if (z < 0) {
            sign = -1;
        }

        z = abs(z) / sqrt(2.0);

        double t = 1.0 / (1.0 + p * z);
        double y = 1.0 - ((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t
                         * exp(-z * z);

        return 0.5 * (1.0 + sign * y);
    }
}
