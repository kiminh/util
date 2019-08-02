/*************************************************************************
    > File Name: train.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 二 12/16 20:19:24 2014
 ************************************************************************/

#include <iostream>
#include <string>
#include <fstream>
#include <ctime>
#include "adPredictor.h"
#include "str_util.h"
#include "sparse_fea.h"
#include "conf_util.h"

void print_help() {
    std::cout << "Usage:"
    << "train fea_num run.conf" << std::endl;
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

    string train_file = bpr_conf.getItem<string>("train_file");
    string model_file = bpr_conf.getItem<string>("valid_file");
    double beta = bpr_conf.getItem<double>(string("beta"));

    algo::adPredictor bpr_model(fea_num, beta);

    //begin train model
    std::ifstream train_stream(train_file);
    string line;
    fea::instance ins;

    std::time_t begin_time;
    time(&begin_time);

    std::cout << "Begin to train model....." << std::endl;
    while (getline(train_stream, line)) {
        bpr_model.parse_line(line, ins);
        bpr_model.train_instance(ins);
    }

    bpr_model.save_model(model_file);
    std::time_t end_time;
    time(&end_time);
    std::cout << "Total train time is : " <<
    difftime(end_time, begin_time) << "seconds." << std::endl;
}
