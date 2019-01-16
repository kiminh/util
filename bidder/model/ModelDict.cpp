#include "ModelDict.h"
#include <string>
#include <fstream>
#include <cmath>
#include <sys/time.h>
namespace model_dict
{
using namespace std;
using namespace alg;

double FtrlModel::score(fea::instance &_instance, double bias)
{
    double inner = this->get_inner(_instance);
    inner += bias;
    return 1 / (1 + exp(-inner));
}

double FtrlModel::get_inner(fea::instance &_instance)
{
    vector <uint32_t> &fea_list = _instance.fea_vec;
    double inner = 0.0;
    vector<uint32_t>::const_iterator iter = fea_list.begin();

    while (iter != fea_list.end()) {
        inner += fea_weight_vec[*iter];
        ++iter;
    }
    return inner;
}

int FtrlModel::init()
{
    return 0;
}

int FtrlModel::load(const char *file_dir, const char *file_name)
{
    if (file_dir == nullptr || file_name == nullptr) {
        return -1;
    }
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);
    char *pos = strstr(filename, "bin");

    if (pos && filename + strlen(filename) - pos == strlen("bin")) {
        cout << "load bin_model: " << filename << endl;
        return this->load_bin(filename);
    } else {
        cout << "load dat_model: " << filename << endl;
        return this->load_dat(filename);
    }
}

int FtrlModel::load_bin(const char *filename)
{
    struct timeval start, end;
    gettimeofday(&start, NULL);

    FILE *fp = fopen(filename, "rb");

    for (uint32_t i = 0; i < 4; i++) {
        double weight = 0.0;
        if (fread(&weight, sizeof(double), 1, fp) != 1) {
            fprintf(stderr, "parse model error!\n");
            return -1;
        }
    }

    uint32_t fea_num = 0;
    if (fread(&fea_num, sizeof(uint32_t), 1, fp) != 1) {
        fprintf(stderr, "parse model get feanum!\n");
        return -1;
    }

    fea_weight_vec.resize(fea_num, 0.0);
    for (uint32_t i = 0; i < fea_num; i++) {
        double w[3];
        if (fread(&w, sizeof(double), 3, fp) != 3) {
            fprintf(stderr, "parse model error!\n");
            return -1;
        }
        fea_weight_vec[i] = w[2];
        //fprintf(stdout,"model\t%u\t%lf\n",i,w[2]);
    }

    gettimeofday(&end, NULL);
    cout << "time_diff: " << (end.tv_sec * 1000 + end.tv_usec / 1000) - (start.tv_sec * 1000 + start.tv_usec / 1000)
         << endl;

    fclose(fp);

    return 0;
}

int FtrlModel::load_dat(const char *filename)
{
    struct timeval start, end;
    gettimeofday(&start, NULL);
    FILE *fp = fopen(filename, "r");

    double alpha;
    double beta;
    double l1reg;
    double l2reg;
    uint32_t fea_num;

    //没有返回值会warning->error无法通过编译
    int ret = fscanf(fp, "%lf", &alpha);
    ret = fscanf(fp, "%lf", &beta);
    ret = fscanf(fp, "%lf", &l1reg);
    ret = fscanf(fp, "%lf", &l2reg);
    ret = fscanf(fp, "%u", &fea_num);

    double w;
    double z = 0;
    double n = 0;
    fea_weight_vec.resize(fea_num, 0.0);
    for (uint32_t index = 0; index < fea_num; ++index) {
        if (fscanf(fp, "%lf %lf %lf", &z, &n, &w)) {
            //fprintf(stdout,"model\t%u\t%lf\n",index,w);
            fea_weight_vec[index] = w;
        }
    }
    gettimeofday(&end, NULL);
    cout << "time_diff: " << (end.tv_sec * 1000 + end.tv_usec / 1000) - (start.tv_sec * 1000 + start.tv_usec / 1000)
         << endl;

    fclose(fp);

    return 0;
}

//lr model store to key-values in unordered_map
float FtrlKVModel::score(fea::instance &_instance, float bias)
{
    float inner = this->get_inner(_instance);
    inner += bias;
    return 1 / (1 + exp(-inner));
}

float FtrlKVModel::get_inner(fea::instance &_instance)
{
    vector <uint32_t> &fea_list = _instance.fea_vec;
    float inner = 0.0;
    vector<uint32_t>::const_iterator iter = fea_list.begin();

    while (iter != fea_list.end()) {
        if(fea_weight_map.count(*iter))
            inner += fea_weight_map[*iter];
        ++iter;
    }
    return inner;
}

int FtrlKVModel::init()
{
    return 0;
}

int FtrlKVModel::load(const char *file_dir, const char *file_name)
{
    if (file_dir == nullptr || file_name == nullptr) {
        return -1;
    }
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);
    char *pos = strstr(filename, "bin");

    if (pos && filename + strlen(filename) - pos == strlen("bin")) {
        cout << "load bin_model: " << filename << endl;
        return this->load_dat(filename);
    } else {
        cout << "load dat_model: " << filename << endl;
        return this->load_dat(filename);
    }
}

int FtrlKVModel::load_bin(const char *filename)
{
    //todo
    return 0;
}

int FtrlKVModel::load_dat(const char *filename)
{
    struct timeval start, end;
    gettimeofday(&start, NULL);
    FILE *fp = fopen(filename, "r");

    float alpha;
    float beta;
    float l1reg;
    float l2reg;

    //没有返回值会warning->error无法通过编译
    int ret = fscanf(fp, "%f", &alpha);
    ret = fscanf(fp, "%f", &beta);
    ret = fscanf(fp, "%f", &l1reg);
    ret = fscanf(fp, "%f", &l2reg);

    uint32_t feaid;
    float w;
    fea_weight_map.clear();

    while(!feof(fp)){
        if(fscanf(fp, "%u %f",&feaid, &w)) {
            if(w != 0.0){
                fea_weight_map[feaid] = w;
                //printf("%u %f\n",feaid,w);
            }
        }
    }

    gettimeofday(&end, NULL);
    cout << "time_diff: " << (end.tv_sec * 1000 + end.tv_usec / 1000) - (start.tv_sec * 1000 + start.tv_usec / 1000)
         << endl;

    fclose(fp);

    return 0;
}

int FMFtrlModel::init()
{
    return 0;
}

int FMFtrlModel::load(const char *file_dir, const char *file_name)
{
    struct timeval start, end;
    gettimeofday(&start, NULL);
    if (file_dir == nullptr || file_name == nullptr) {
        return -1;
    }
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);
    cout << "load fm " << filename << endl;
    FILE *fp = fopen(filename, "r");

    //没有返回值会warning->error无法通过编译
    int ret = fscanf(fp, "%u", &this->fea_num);
    fea_weight_vec.resize(this->fea_num, 0.0);
    ret = fscanf(fp, "%u", &this->fm_dim);
    cout << "get fea_name: " << fea_num << " " << fm_dim << endl;
    vector<double> empty_vec;
    fm_weight_vec.resize(this->fea_num, empty_vec);
    for (uint32_t index = 0; index < fea_num; ++index) {
        uint32_t fea_cnt = 0;
        double w;
        if (fscanf(fp, "%u,%lf", &fea_cnt, &w)) {
            fea_weight_vec[fea_cnt] = w;
        } else {
            fprintf(stderr, "load model error!\n");
            return -1;
        }
        vector<double> fm_vec;
        for (uint32_t i = 0; i < fm_dim; i++) {
            if (fscanf(fp, ",%lf", &w)) {
                fm_vec.push_back(w);
            } else {
                fprintf(stderr, "load model error!\n");
                return -1;
            }
        }
        fm_weight_vec[fea_cnt] = fm_vec;
    }
    gettimeofday(&end, NULL);
    cout << "time_diff: " << (end.tv_sec * 1000 + end.tv_usec / 1000) - (start.tv_sec * 1000 + start.tv_usec / 1000)
         << endl;

    fclose(fp);

    return 0;
}

double FMFtrlModel::score(fea::instance &_instance, double bias)
{
    vector <uint32_t> &fea_list = _instance.fea_vec;
    double inner = fea_weight_vec[0];
    vector<uint32_t>::const_iterator iter = fea_list.begin();

    while (iter != fea_list.end()) {
        inner += fea_weight_vec[*iter];
        ++iter;
    }

    for (uint32_t i = 0; i < fea_list.size(); i++) {
        uint32_t x = fea_list[i];
        for (uint32_t j = i + 1; j < fea_list.size(); j++) {
            uint32_t y = fea_list[j];
            for (uint32_t k = 0; k < this->fm_dim; k++) {
                inner += fm_weight_vec[x][k] * fm_weight_vec[y][k];
            }
        }
    }

    inner += bias;
    return 1 / (1 + exp(-inner));
}

int GBDTModel::init()
{
    return 0;
}

int GBDTModel::load(const char *file_dir, const char *file_name)
{

    if (file_dir == nullptr || file_name == nullptr) {
        return -1;
    }
    char filename[512];
    snprintf(filename, 512, "%s/%s", file_dir, file_name);

    if (!m_gbdt.load_tree_model(filename)) {
        cerr << "load tree model error!" << endl;
        return -1;
    }

    return 0;
}

int GBDTModel::join_fea(fea::instance &ins, std::vector <uint32_t> &gbdt_out, uint32_t gbdt_hash_bias)
{

    m_gbdt.get_trees_leaf_index(ins, gbdt_out);

    for (auto iter = gbdt_out.begin(); iter != gbdt_out.end(); ++iter) {
        ins.fea_vec.push_back(*iter + gbdt_hash_bias);
        cout << "add gbdt_fea: " << *iter << endl;

    }
    return 0;
}

double GBDTModel::score(fea::instance &ins)
{
    return m_gbdt.predict_line(ins);
}

}
