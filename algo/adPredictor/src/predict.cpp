/*************************************************************************
    > File Name: predict.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 二 12/16 20:19:24 2014
 ************************************************************************/

#include <iostream>
#include <string>
#include <fstream>
#include <ctime>
#include "sparse_fea.h"
#include "adPredictor.h"
#include "str_util.h"
#include "conf_util.h"

void print_help() {
    std::cout << "Usage:"
    << "predict fea_num run.conf" << std::endl;
    exit(0);
}

int main(int argc, char *argv[]) {
    using namespace std;

    if (argc != 3) {
        print_help();
    }

    util::str_util m_str_util;
    int fea_num = m_str_util.castFromS<int>(argv[1]);

    if (fea_num <= 0) {
        std::cout << "fea_num seting is not correct, must be positive" << std::endl;
        exit(1);
    }

    string conf_file(argv[2]);
    util::conf_util bpr_conf;
    bpr_conf.parse(conf_file);

    string valid_file = bpr_conf.getItem<string>("valid_file");
    string model_file = bpr_conf.getItem<string>("model_file");
    string predict_file_out = bpr_conf.getItem<string>("predict_out");

    double beta = bpr_conf.getItem<double>(string("beta"));

    algo::adPredictor bpr_model(fea_num, beta);

    std::time_t begin_time;
    time(&begin_time);

    ofstream out_file(predict_file_out);

    //begin predict
    std::ifstream predict_stream(predict_file_out);
    string line;
    fea::instance ins;

    std::cout << "Begin to predict ....." << std::endl;
    while (getline(predict_stream, line)) {
        bpr_model.parse_line(line, ins);
        double score = bpr_model.predict_instance(ins);
        out_file << score << " " << line << endl;
    }

    bpr_model.save_model(model_file);
    std::time_t end_time;
    time(&end_time);
    std::cout << "Total train time is : " <<
    difftime(end_time, begin_time) << "seconds." << std::endl;
}
