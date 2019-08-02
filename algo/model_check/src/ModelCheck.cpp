#include <string>
#include <fstream>
#include <cmath>
#include <sys/time.h>
#include "str_util.h"
#include "conf_util.h"
#include <iostream>

#define EPSILON 1e-7

namespace model_check {

    using namespace std;


    int is_zero(double num){
        
        if(num < EPSILON && num > (-1)*EPSILON){
            return 1;    
        }  
        return 0;
    }

    int check_model(const char* model_file, const char* conf_file, std::string& reason){

        util::conf_util adfea_conf;
        adfea_conf.parse(conf_file);
         
        uint32_t beta_index = 7242330;
        if(adfea_conf.has_item("beta_index"))
        {   
            beta_index = adfea_conf.getItem<uint32_t>("beta_index");
        }

        double weight_ratio = 0.6;
        if(adfea_conf.has_item("weight_ratio"))
        {   
            weight_ratio = adfea_conf.getItem<double>("weight_ratio");
        }

        FILE *fp = fopen(model_file,"r");

        double alpha;
        double beta;
        double l1reg;
        double l2reg;
        uint32_t fea_num;

        //没有返回值会warning->error无法通过编译
        int ret = fscanf(fp,"%lf",&alpha);
        ret = fscanf(fp,"%lf",&beta);
        ret = fscanf(fp,"%lf",&l1reg);
        ret = fscanf(fp,"%lf",&l2reg);
        ret = fscanf(fp,"%u",&fea_num);

        double w;
        double z = 0;
        double n = 0;

        std::vector<double> fea_weight_vec;
        fea_weight_vec.resize(fea_num, 0.0);


        uint32_t nozero_fea = 0;
        for(uint32_t index = 0; index < fea_num; ++index){

            if(fscanf(fp,"%lf %lf %lf",&z,&n,&w) > 0){
                if(index == beta_index){
                    char fea_buf[1024];
                    snprintf(fea_buf,1024,"beta0 weight[%f]",w);
                    reason += string(fea_buf);
                }
                if(is_zero(w)){
                    if(index == beta_index){
                       char fea_buf[1024];
                        snprintf(fea_buf,1024,"\tbeta0[%u] has no weight! Error!",beta_index);
                        reason += string(fea_buf);
                        return -1;
                    }
                }else{
                    nozero_fea +=1;
                }
                fea_weight_vec[index] = w;
            }else{
                char fea_buf[1024];
                snprintf(fea_buf,1024,"\tmodel_line:[%u] is less than correct fea_num[%u]!",index,fea_num);
                reason += string(fea_buf);
                return -1;
            }
        
        }
        double nozero_ratio = double(nozero_fea)/fea_num;
        if(nozero_ratio < weight_ratio){
             char fea_buf[1024];
             snprintf(fea_buf,1024,"\tfea(have weights)/total_fea = [%f] is too small(<%f)", nozero_ratio,weight_ratio);
             reason += string(fea_buf);
             return -1;
        }
        cout << "final pass!" << endl;
        return 0;
    }

}

    int main(int argc, char* argv[])
    {
        using namespace util;
        using namespace std;

        if(argc < 4)
        {   
            fprintf(stderr, "Usage: %s model check_conf output\n", argv[0]);
            return 0;
        }   
        string reason("");
        int ret = model_check::check_model(argv[1], argv[2], reason);   
        FILE *fo = fopen(argv[3],"w");
        fprintf(fo,"check fail:[%s]\n", reason.c_str());
        return ret;
    }


