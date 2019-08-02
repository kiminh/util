#include "FM_FTRL_machine.h"

namespace FM_FTRL {
    using namespace std;
    double get_diff(time_b time2,time_b time1){
            return (time2.time - time1.time)*1000 + (time2.millitm - time1.millitm);
    }

    double logloss(double p,double y){
        p = max(min(p, 1. - 1e-15), 1e-15);
        if( y-1 < EPSILON && y-1 >EPSILON*(-1)){
            return (-1)*log(p);    
        }else{
            return (-1)*log(1.-p);
        }
        
    }

    int FMFtrlModel::init_fm(uint32_t index){
        //n_fm       
        //this->n_fm 的index为d的特征
        if(index >= this->n_fm.size()){
            cerr << "index oversize n_fm" << endl;
            return -1;
        }
        //n_fm[index]长度为空
        //cout << "n_fm " << index << " size:" << n_fm[index].size() << endl;
        if(this->n_fm[index].size() == this->fm_dim){
            //cout << "feature index:" << index << " has already initized" << endl;    
            return 0;
        }

        //vector<double> sub_vec(this->fm_dim,0.0);
        this->n_fm[index].resize(this->fm_dim,0.0);
        //this->n_fm[index] = sub_vec;
        this->z_fm[index].resize(this->fm_dim,0.0);
        //this->z_fm[index] = sub_vec;
        this->w_fm[index].resize(this->fm_dim,0.0);
        //this->w_fm[index] = sub_vec;

           //random.gauss(mu,sigma)    # 随机生成符合高斯分布的随机数，mu,sigma为高斯分布的两个参数。  
           //gaussss_distribution
        //cout << "init feature " << index << endl;
        for(uint32_t k = 0; k < this->fm_dim;  k++){
            this->z_fm[index][k] = (*p_gauss_distribution)(generator);
        }
        return 0;
    }

    
    

    double FMFtrlModel::predict_raw(vector<uint32_t> fea_vec){

        time_b time0;
        ftime(&time0);
         
        w[0] = this->z[0]*(-1)/((this->beta + sqrt(this->n[0]))/this->alpha);
        double raw_y = w[0];


        for(size_t i = 0;i < fea_vec.size();i++){
            uint32_t fea = fea_vec[i];
            double sign = (z[fea] < 0 ? -1.:1.);
            if(sign * z[fea] <= L1){
                w[fea] = 0.0;
            }else{
                w[fea] = (sign * L1 - z[fea])/((beta + sqrt(n[fea]))/alpha + L2);
                //w[fea] = (sign * L1 - z[i]) / ((beta + sqrt(n[i])) / alpha + L2)   
            }
            raw_y += this->w[fea];
        }
    
        for(size_t i = 0;i < fea_vec.size();i++){
            uint32_t fea = fea_vec[i];
            this->init_fm(fea);
            for(size_t k = 0; k < this->fm_dim;k++){
                //cout << "init w_fm " << fea << " k=" << k << " " << endl;
                double sign = (z_fm[fea][k] < 0 ? -1.:1.);
                if(sign * z_fm[fea][k] <= L1_fm){
                    w_fm[fea][k] = 0.;
                }else{
                    //w_fm[fea][k] = (sign * L1_fm - z_fm[i][k]) / ((beta_fm + sqrt(n_fm[i][k])) / alpha_fm + L2_fm)
                    w_fm[fea][k] = (sign * L1_fm - z_fm[fea][k]) / ((beta_fm + sqrt(n_fm[fea][k]))/alpha_fm + L2_fm);
                }
            }
            
        }

        for(size_t i = 0 ;i < fea_vec.size();i++){
            uint32_t fea_x = fea_vec[i];
            for(size_t j = i+1; j< fea_vec.size();j++){
                uint32_t fea_y = fea_vec[j];
                for(size_t k =0; k < fm_dim;k++){
                    raw_y += w_fm[fea_x][k] * w_fm[fea_y][k];
                }    
            }    
        }
        time_b time1;
        ftime(&time1);
        //cout << "predict_time:" << get_diff(time1,time0) << endl;
        return raw_y;

    }

    double FMFtrlModel::predict(vector<uint32_t> fea_vec){
        return 1./(1. + exp((-1)* max(min(this->predict_raw(fea_vec),35.),-35.)) );
    }
    
    int FMFtrlModel::update(vector<uint32_t> fea_vec,double p, double y){  

        time_b time0;
        ftime(&time0);
        //fm sum不需清零
        //this->fm_sum.clear();
        //std::vector<double>emtpy_vec;
        //this->fm_sum.resize(D+1,emtpy_vec);

        double g = p - y;

        double sigma = 0.0;
        for(size_t i = 0; i < fea_vec.size();i++){
            uint32_t fea = fea_vec[i];
            sigma = (sqrt(n[fea] + g * g) - sqrt(n[fea])) / alpha; 
            z[fea] += g - sigma * w[fea];
            n[fea] += g * g;
            fm_sum[fea].resize(fm_dim,0.0);
        }
        fm_sum[0].resize(fm_dim,0.0);

        sigma = (sqrt(n[0] + g * g) - sqrt(n[0])) / alpha;
        z[0] += g - sigma * w[0];
        n[0] += g * g;
        time_b time2;
        ftime(&time2);


        for(size_t i = 0; i < fea_vec.size(); i++){
            uint32_t fea_x = fea_vec[i];
            if(fm_sum[fea_x].empty()){
                fm_sum[fea_x].resize(fm_dim,0.0);
            }
            for(size_t j = 0; j < fea_vec.size();j++){
                uint32_t fea_y = fea_vec[j];
                //if(fm_sum[fea_y].empty()){
                //    fm_sum[fea_y].resize(fm_dim,0.0);
                //}
                if(i != j){
                    for(size_t k = 0; k < fm_dim;k++){
                        fm_sum[fea_x][k] += w_fm[fea_y][k];
                    }    
                }
            }    
        }
        time_b time3;
        ftime(&time3);
        for(size_t i = 0; i < fea_vec.size(); i++){ 
            uint32_t fea = fea_vec[i];
            for(size_t k = 0; k < fm_dim; k++){
                double g_fm = g * fm_sum[fea][k];
                sigma = (sqrt(n_fm[fea][k] + g_fm * g_fm) - sqrt(n_fm[fea][k])) / alpha_fm;
                z_fm[fea][k] += g_fm - sigma * w_fm[fea][k];
                n_fm[fea][k] += g_fm * g_fm;
            }
            
        }
        time_b time4;
        ftime(&time4);
        //cout << "parse_time:" << get_diff(time2,time0)
        //                     << " update_time:" << get_diff(time3,time2)<<  
        //                 " last update: " << get_diff(time4,time3) << endl;
        return 0;
    }
/*
void print_featext(std::ofstream& ofs, vector<string>& fea_res,int label)
{
        ofs << label;
            //fea::fea_list& fea_items = fea_res.fea_vec; 
                for(vector<string>::iterator iter = fea_res.begin(); iter!=fea_res.end();++iter)
                        {   
                                    ofs << " " << *iter;
                                        }   
                                            ofs << "\n";
}
*/
    int FMFtrlModel::dump_model(ofstream &ofs){
        //line_1: LR_model.size
        //w.size()
        ofs << this->D+1 << endl;
        ofs << this->fm_dim << endl;
        for(size_t i = 0; i < this->w.size();i++){
            ofs << i;
            double weight_LR = w[i];
            ofs << "," << weight_LR;
            vector<double> weight_FM_vec = w_fm[i];
            //cout << "the w_fm:i " << i << " subsize: " << weight_FM_vec.size() << endl;
            size_t j = 0;
            for(;j < weight_FM_vec.size();j++){
                //1,0.000000,0.0,0.0,0.0
                ofs << "," << weight_FM_vec[j];
            }
            while(j<fm_dim){
                ofs << ",0.0";   
                j++;
            }
            ofs << endl;
        }
        
        return 0;    
    }
    
}
