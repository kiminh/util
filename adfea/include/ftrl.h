/*************************************************************************
    > File Name: Ftrl.h
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 二 12/16 15:42:20 2014
	> For online leaning 
	> This is the online learning logistic
	> paper :Ad Prediction: a View from the Trenches
 ************************************************************************/
#ifndef FTRL_H
#define FTRL_H

#include <string>
#include <vector>
#include <map>
#include <cmath>
#include "str_util.h"
#include "sparse_fea.h"

namespace algo {
    class Ftrl {
    public:
        Ftrl();

        Ftrl(double alpha, double beta, double l1reg, double l2reg, uint32_t fea_num);

    public:
        bool train_instance(fea::instance &ins);

        /**line format:
         * label:
         * feaid1 feaid2+
         */
        bool train_line(std::string &line);

    public:
        double predict_instance(fea::instance &ins);

        double predict_line(std::string &line);

        double predict_instance_static(fea::instance &);

    public:
        bool save_model(std::string &file);

        bool load_static_model(std::string &file);

        bool load_model(std::string &file);

        void free();

    public:
        bool parse_line(std::string &line);

        bool parse_line(std::string &line, fea::instance &ins);

    private:
        inline double sigmod(double total_weight);

        double inner_product(std::vector<uint32_t> &fea_vec);

        inline int sign(double value) {
            return value > 0 ? 1 : -1;
        }

    private:
        double alpha;
        double beta;
        double l1reg;
        double l2reg;

    private:
        uint32_t fea_num;
        double *z;
        double *n;
        double *w;
        //fea weight
        std::map<int, double> fea_weight_map;
    };
}

#endif
