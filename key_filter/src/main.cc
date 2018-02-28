#include <iostream>
#include "keyfilter.h"
int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <inputfile> <outputfile> para=val\n", argv[0]);
        return EXIT_FAILURE;
    }

    algo::KeyFilter *kf = new algo::KeyFilter();
    kf->SetParam("input_file",argv[1]);
    kf->SetParam("output_file",argv[2]);

    char name[128],val[128];
    for(int i = 3; i < argc;++i)
    {
      if(sscanf(argv[i], "%[^=]=%s", name, val) == 2) {
        kf->SetParam(name, val);
      }
    }
    kf->Init();
    kf->Filter();
    if(kf != nullptr){
      delete kf;
      kf = nullptr;
    }
}
