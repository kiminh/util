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
    FILE *fp_in = fopen(infile,"r");
    FILE *fp_out = fopen(outfile,"wb");

    double alpha;
    fscanf(fp_in,"%lf",&alpha);
    fwrite(&alpha,sizeof(double),1,fp_out);

    double beta;
    fscanf(fp_in,"%lf",&beta);
    fwrite(&beta,sizeof(double),1,fp_out);

    double l1reg;
    fscanf(fp_in,"%lf",&l1reg);
    fwrite(&l1reg,sizeof(double),1,fp_out);

    double l2reg;
    fscanf(fp_in,"%lf",&l2reg);
    fwrite(&l2reg,sizeof(double),1,fp_out);

    uint32_t fea_num;
    fscanf(fp_in,"%u",&fea_num);
    fwrite(&fea_num,sizeof(uint32_t),1,fp_out);

    for(uint32_t i = 0;i < fea_num;i++){ 
        double w[3];
        fscanf(fp_in,"%lf %lf %lf",&w[0],&w[1],&w[2]);
        fwrite(&w,sizeof(double),3,fp_out);
       // fwrite(&n,sizeof(double),1,fp_out);
       // fwrite(&w,sizeof(double),1,fp_out);
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
        fprintf(stderr,"Usage: %s model_text model_bin\n",argv[0]);
        return 0;
    }
    int ret = process(argv[1],argv[2]);
    if(ret!=0){
        fprintf(stderr,"Error parse model!\n");
        return ret;
        
    }
    return 0;
}
