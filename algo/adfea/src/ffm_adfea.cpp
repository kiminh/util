#include <ctime>
#include <iostream>
#include <fstream>
#include "extractor.h"
#include "sparse_fea.h"
#include "conf_util.h"

void print_help()
{
    std::cout <<"Usage:" << "ffm_adfea adfea.conf" << std::endl;
    exit(0);
}

void print_fea(std::ofstream& ofs, fea::instance& fea_res)
{
    ofs << fea_res.label;
    fea::fea_list& fea_items = fea_res.fea_vec;
    for(uint32_t index = 0; index < fea_items.size(); ++index)
    {
        ofs << " " << fea_items[index].slot_index << ":" << fea_items[index].fea_index << ":1";
    }
    ofs << "\n";
}

void print_onlyfea(std::ofstream& ofs,map<string,uint32_t>feature_dict){
    for(map<string,uint32_t>::iterator iter = feature_dict.begin();iter!=feature_dict.end();++iter){
        ofs << iter->first << "\t" << iter->second << endl;
    }

}

void print_fea(std::ofstream& ofs, fea::instance& fea_res,bool debug=false)
{
    if(!fea_res.key.empty()){
        ofs << fea_res.key << " ";
    }
    ofs << fea_res.label;
    fea::fea_list& fea_items = fea_res.fea_vec;
    for(uint32_t index = 0; index < fea_items.size(); ++index)
    {
        if(debug)
        {
            ofs << " " << fea_items[index].fea_name << ":" << fea_items[index].fea_index;
        }
        else
        {
            ofs << " " << fea_items[index].fea_index;
        }
    }
    ofs << "\n";
}

int main(int argc, char* argv[])
{
    using namespace util;
    using namespace std;

    if(argc != 2)
    {
        print_help();
    }

    util::conf_util adfea_conf;
    adfea_conf.parse(argv[1]);

    string train_ins_file;
    if(adfea_conf.has_item("train_instance"))
    {
        train_ins_file = adfea_conf.getItem<string>("train_instance");
    }
    ofstream output_stream(train_ins_file);
    cout << "train_ins_file:" << train_ins_file << endl;

    fea::extractor fea_extractor;

    if(!fea_extractor.init(argv[1]))
    {
        cout << "extractor init error!" << endl;
        return -1;
    }

    bool debug = false;
    if(adfea_conf.has_item("debug_mode"))
    {
        debug = adfea_conf.getItem<bool>("debug_mode");
    }
    bool only_fea = false;
    if(adfea_conf.has_item("only_fea"))
    {
        only_fea = adfea_conf.getItem<bool>("only_fea");
    }
    cout << "only_fea :" << only_fea << endl;

    bool belta0_out = false;
    if(adfea_conf.has_item("belta0_out"))
    {
        belta0_out = adfea_conf.getItem<bool>("belta0_out");
    }
    string belta0_file;
    if(belta0_out)
    {
        belta0_file = adfea_conf.getItem<string>("belta0_file");
    }

    cout << "betal 0 " << belta0_out << " " << belta0_file << endl;

    string train_files;
    if(adfea_conf.has_item("train_file"))
    {
        train_files = adfea_conf.getItem<string>("train_file");
    }
    else
    {
        cerr << "get adfea input error" << endl;
        return -1;
    }

    vector<string> train_file_vec;
    util::str_util::split(train_files, ",",train_file_vec);

    map<string,uint32_t> feature_dict;

    string line;
    fea::instance result;
    vector<string> vec;

    cout << "begin to adfea...." << endl;
    std::time_t begin_time;
    time(&begin_time);

    while(getline(input_stream, line))
    {
        vec.clear();
        util::str_util::trim(line);
        util::str_util::split(line, "\001", vec);
        fea_extractor.record_reset();
        if(!fea_extractor.add_record(vec))
        {
            cout << "add record error" << endl;
            cout << line << endl;
            for(size_t i = 0; i < vec.size(); ++i)
            {
                cout << i << " " << vec[i] << " ";
            }
            cout << endl;
            continue;
        }
        fea_extractor.extract_fea();
        fea_extractor.get_fea_result(result);
        print_fea(output_stream, result);
    }

    fea_extractor.clear();

    std::time_t end_time;
    time(&end_time);
    std::cout << "AdFea cost time: "
    << difftime(end_time, begin_time) << "seconds." << std::endl;
    input_stream.close();
    output_stream.close();
}
