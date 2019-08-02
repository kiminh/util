#ifndef FM_FTRL_MACHINE_H
#define FM_FTRL_MACHINE_H

#include <map>
#include <iostream>
#include <stdlib.h>
#include <math.h>
#include <string>
#include <fstream>
#include <cmath>
#include <random>
#include <sys/timeb.h>



namespace FM_FTRL {



typedef struct timeb time_b;
    





#define EPSILON 1e-6

    struct FMFtrlModel {
        // alpha, beta, alpha_fm = alpha_fm, beta_fm = beta_fm
        int init(uint32_t fm_dim, double fm_initDev,double L1,double L2,
            double L1_fm, double L2_fm,uint32_t D,
            double alpha, double beta,double alpha_fm,double beta_fm){
            this->fm_dim = fm_dim;
            this->fm_initDev = fm_initDev;
            this->L1 = L1;
            this->L2 = L2;
            this->L1_fm = L1_fm;
            this->L2_fm = L2_fm;

            this->D = D;
        
            this->alpha = alpha;
            this->beta = beta;
            this->alpha_fm = alpha_fm;
            this->beta_fm = beta_fm;
            
            this->n.resize(D+1,0.0);
            this->z.resize(D+1,0.0);
            this->w.resize(D+1,0.0);

            std::vector<double>emtpy_vec;
            this->n_fm.resize(D+1,emtpy_vec);
            this->w_fm.resize(D+1,emtpy_vec);
            this->z_fm.resize(D+1,emtpy_vec);

            this->fm_sum.resize(D+1,emtpy_vec);


            p_gauss_distribution = new std::normal_distribution<double>(0.0,fm_initDev);

    
            return 0;
            }

        ~FMFtrlModel(){
            n.clear();
            z.clear();
            w.clear();
            n_fm.clear();
            w_fm.clear();
            z_fm.clear();
            if(p_gauss_distribution){
                delete p_gauss_distribution;
                p_gauss_distribution = NULL;
            }
        }

        std::default_random_engine generator;
        std::normal_distribution<double>* p_gauss_distribution;

        //std::map<uint32_t, double> fea_weight_map;

        std::vector<double> n;
        std::vector<double> z;
        std::vector<double> w;

        std::vector<std::vector<double> > n_fm;
        std::vector<std::vector<double> > w_fm;
        std::vector<std::vector<double> > z_fm;

        std::vector<std::vector<double> > fm_sum;

        
        uint32_t fm_dim;
        double fm_initDev;
        double L1;
        double L2;
        double L1_fm;
        double L2_fm;
        double alpha;
        double beta;
        uint32_t D;
        double alpha_fm;
        double beta_fm;

        //i is hashed feature_sign
        int init_fm(uint32_t i);
        double predict_raw(std::vector<uint32_t> x);
        double predict(std::vector<uint32_t> x);
        
        int update(std::vector<uint32_t>, double,double);
        int dump_model(std::ofstream &);
    };

    double logloss(double,double);

}


#endif //FM_FTRL_MACHINE_H
