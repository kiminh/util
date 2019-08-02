/*************************************************************************
    > File Name: extract_main.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 五 12/19 23:01:12 2014
 ************************************************************************/
#include <ctime>
#include "FM_FTRL_machine.h"
#include "str_util.h"
#include <sys/timeb.h>  
#include "conf_util.h"


void print_help()
{
	std::cout <<"Usage:" << "train_FMFtrl input model conf" << std::endl;
	exit(0);
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


void print_fea(std::ofstream& ofs, fea::instance& fea_res)
{
	ofs << fea_res.label;
	fea::fea_list& fea_items = fea_res.fea_vec; 
	for(uint32_t index = 0; index < fea_items.size(); ++index)
	{
		ofs << " " << fea_items[index].fea_index;
	}
	ofs << "\n";
}
*/


double g_L1_fm = 2.0;
double g_L2_fm = 3.0;

int main(int argc, char* argv[])
{
	using namespace std;
    using namespace util;
    using namespace FM_FTRL;

	if(argc < 4)
	{
		print_help();
	}
	/*
    util::conf_util adfea_conf;
    adfea_conf.parse(argv[1]);

	string train_file;
	if(adfea_conf.has_item("train_file"))
	{
		train_file = adfea_conf.getItem<string>("train_file");
	}
    */
    //read conf
    util::conf_util fm_conf;
    fm_conf.parse(argv[3]);
    uint32_t fm_dim  = fm_conf.getItem<uint32_t>("fm_dim");
    double fm_initDev  = fm_conf.getItem<double>("fm_initDev");
    double L1 = fm_conf.getItem<double>("L1");
    double L2 = fm_conf.getItem<double>("L2");
    uint32_t D = fm_conf.getItem<uint32_t>("D");
    double L1_fm = fm_conf.getItem<double>("L1_fm");
    double L2_fm = fm_conf.getItem<double>("L2_fm");
    double alpha = fm_conf.getItem<double>("alpha");
    double beta = fm_conf.getItem<double>("beta");
    double alpha_fm = fm_conf.getItem<double>("alpha_fm");
    double beta_fm = fm_conf.getItem<double>("beta_fm");

    cout << "fm_dim: " << fm_dim << endl;
    cout << " fm_initDev:" << fm_initDev << endl;
    cout << " L1: " << L1 << " L2:" << L2 << " D:" << D << " L1_fm:" << L1_fm << " L2_fm:" << L2_fm << " alpha:" << alpha << " beta:" << beta << " alpha_fm:" << alpha_fm << " beta_fm: " << beta_fm << endl;


    FMFtrlModel learner;;
    learner.init(fm_dim,fm_initDev,
        L1,L2,
        L1_fm,L2_fm,
        D,
        alpha,beta,
        alpha_fm,beta_fm);

	std::time_t begin_time;
	time(&begin_time);
    for(int e = 0; e < 5;e++){
        if(e==0){
            learner.L1_fm = 0.;
            learner.L2_fm = 0.;
        }else{
            learner.L1_fm = L1_fm;    
            learner.L2_fm = L2_fm;    
        }  
        double progressiveLoss = 0.;
        double progressiveCount = 0.;

        int index = 0;
        //y is label x is feature_vec
        string line;
	    ifstream input_stream(argv[1]);
        //time_b time3;
        while(getline(input_stream, line))
        {

            time_b time0; 
            //std::time_t time0;
            ftime(&time0);
            //cout<<t1.millitm<<endl;

            vector<string> vec;
            vec.clear();
            util::str_util::trim(line);
            util::str_util::split(line, " ", vec);
            //feature label fea1 fea2
            double y = atof(vec[0].c_str());
            //cout << "get y " << y << endl;
            vector<uint32_t> fea_vec;
            for(size_t i = 1; i < vec.size();i++){
                //strtoul(vec[1].c_str(), NULL, 0);
                //cout << "push feature " << strtoul(vec[i].c_str(),NULL,0) << endl;
                fea_vec.push_back(strtoul(vec[i].c_str(),NULL,0));    
            }
            //std::time_t time1;
            time_b time1;
            ftime(&time1);
            //cout << "begin predict " << endl;
            double p = learner.predict(fea_vec);
            //cout << "after predict " << endl;
            //std::time_t time2;
            //time(&time2);
            time_b time2;
            ftime(&time2);
            double loss = logloss(p,y);
            learner.update(fea_vec,p,y);
            //cout << "after update" << endl;
            //std::time_t time3;
            //time(&time3);
            time_b time3;
            ftime(&time3);

            /*           
            if(index == 0){
                cout << "parse_time:" << get_diff(time1,time0) << " predict_time:" <<  get_diff(time2,time1)
                     << " update_time:" << get_diff(time3,time2)<< endl;
            }else{
                cout << "jump_time:" << get_diff(time3,time0) << " parse_time:" << get_diff(time1,time0) << " predict_time:" <<  get_diff(time2,time1)
                     << " update_time:" << get_diff(time3,time2)<< endl;
            }
            */
            
            
              //  << time2 - time1<< " update_time:" << time3 - time2 << endl;


            progressiveLoss += loss;
            progressiveCount += 1.;
            index += 1;
            if(index%10000 == 0){
                cout << "Epoch " << e << "\tcount: " << index << " Processive Loss: " << progressiveLoss / progressiveCount << endl;    
            }

        }
	//std::time_t end_time;
	//time(&end_time);
       // cout << "Epoch " << i  << " Processive Loss: " << progressiveLoss / progressiveCount <<  " elapsed time: " << difftime(end_time, begin_time) << "seconds" <<  endl;    
    }

	ofstream output_stream(argv[2]);
    learner.dump_model(output_stream);  

	//std::cout << "AdFea cost time: " 
	//	<< difftime(end_time, begin_time) << "seconds." << std::endl;
	//input_stream.close();
	//output_stream.close();
    return 0;
}
