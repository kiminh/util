#include "ModelDict.h"
#include <string>
#include <fstream>
#include <cmath>

namespace model_dict {
    using namespace std;
    using namespace util;

    int FMFtrlModel::init(){
        return 0;
    }


    int FMFtrlModel::load(const char *file_name){
        if (file_name == nullptr) {
            return -1;

        }
        std::ifstream model_file(file_name);

        string line;
        //feature , LR_weight, FM_weight1,FM_weight2... FM_weight{fm_dim}

        //fm_weigh_vec清空

        uint32_t line_cnt = 0;
        uint32_t fea_cnt = 0;
        while (getline(model_file, line)) {
            util::str_util::trim(line);
            line_cnt += 1;
            //第一行是feature cnt
            //第二行是fm_dim
            if(line_cnt == 1){
                this->fea_num = strtoul(line.c_str(), NULL, 0);
            }else if(line_cnt == 2){
                this->fm_dim = strtoul(line.c_str(), NULL, 0);
                fea_weight_vec.resize(this->fea_num, 0.0);
                //vector<double>emtpy_vec(this->fm_dim,0.0);
                vector<double>emtpy_vec;
                fm_weight_vec.resize(this->fea_num,emtpy_vec);
            }else{
                vector<string> vec;
                util::str_util::split(line,",", vec);
                if(vec.size() < this->fm_dim + 2 ){
                    cerr << "load model_ftrl error:" << line << endl;
                    continue;
                }
                fea_weight_vec[fea_cnt] = atof(vec[1].c_str());
                vector<double> fm_vec;
                for(uint32_t i = 2; i < this->fm_dim + 2; ++i){
                    fm_vec.push_back(atof(vec[i].c_str()));
                }
                fm_weight_vec[fea_cnt] = fm_vec;
                fea_cnt += 1;
            }

        }
        return 0;
    }

    double FMFtrlModel::score(vector<uint32_t>fea_list,double bias){
        //vector<uint32_t>& fea_list = _instance.fea_vec;

        double inner = fea_weight_vec[0];
        vector<uint32_t>::const_iterator iter = fea_list.begin();

        while(iter != fea_list.end()){
            inner += fea_weight_vec[*iter];
            ++iter;
        }

        for(uint32_t i = 0 ; i< fea_list.size();i++){
            uint32_t x = fea_list[i];
            for(uint32_t j = i +1; j < fea_list.size();j++){
                uint32_t y = fea_list[j];
                for(uint32_t k = 0; k < this->fm_dim; k++){
                    inner += fm_weight_vec[x][k] * fm_weight_vec[y][k];
                }
            }
        }

        inner += bias;
        return 1 / (1 + exp(-inner));
    }

}

using namespace model_dict;
using namespace std;
using namespace util;
    int main(int argc, char* argv[])
    {
        if(argc <4){
            cout << "Usage: bin model_file test output"<< endl;
        }

        FMFtrlModel_t fmftrl_data;
        fmftrl_data.load(argv[1]);

        ifstream infile(argv[2]);
        ofstream outfile(argv[3]);
        
        string line;
        vector<string> vec;
        vector<uint32_t> fea_vec;
        while(getline(infile, line)){
            vec.clear();
            util::str_util::trim(line);
            util::str_util::split(line, " ", vec);
            fea_vec.clear();
            for(uint32_t i = 1; i< vec.size();i++){
                //strtoul(line.c_str(), NULL, 0);
                fea_vec.push_back(strtoul(vec[i].c_str(), NULL, 0));
            }
            double pctr = fmftrl_data.score(fea_vec,0.0);
            outfile << pctr << " " <<  line << endl;
        }
        

        return 0;
    }

