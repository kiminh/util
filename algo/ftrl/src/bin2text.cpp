/*************************************************************************
    > File Name: extract_main.cpp
    > Author: starsnet83
    > Mail: starsnet83@gmail.com 
    > Created Time: 五 12/19 23:01:12 2014
 ************************************************************************/
#include <ctime>
#include <iostream>
#include <fstream>
#include<stdio.h>
#include <stdint.h>
//#include<time.h>
//#include <sys/time.h>


int process(const char* infile,const char* outfile){

    uint32_t index = 0;
    FILE *fp_in = fopen(infile,"rb");
    FILE *fp_out = fopen(outfile,"w");


    for(uint32_t i=0; i < 4;i++){ 
        double weight = 0.0;
        fread(&weight,sizeof(double),1,fp_in);
        fprintf(fp_out,"%lf\n",weight);
    }
    uint32_t fea_num = 0;
    fread(&fea_num,sizeof(uint32_t),1,fp_in);
    fprintf(fp_out,"%lu\n",fea_num);

    for(uint32_t i = 0;i < fea_num;i++){ 
        double w[3];
        fread(&w,sizeof(double),3,fp_in);
        fprintf(fp_out,"%lf %lf %lf\n",w[0],w[1],w[2]);
    }


    fclose(fp_in);
    fclose(fp_out);

    return 0;   
}


int main(int argc, char* argv[])
{
    using namespace std;

    if(argc < 3)
    {
        fprintf(stderr,"Usage: %s model_bin model_text\n",argv[0]);
        return 0;
    }
    int ret = process(argv[1],argv[2]);
    if(ret!=0){
        fprintf(stderr,"Error parse model!\n");
        return ret;
        
    }
    return 0;
}
