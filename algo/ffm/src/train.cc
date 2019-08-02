#include "../include/ftrl_train.h"

int main(int argc,char **argv)
{
  if(argc < 2)
  {
    std::cout << "Usage:train_data param=val" << std::endl;
    return 0;
  }
 
  BayesAlgo::FTRL *ftrl = new BayesAlgo::FTRL(argv[1]);
  
  char name[256],val[256];
  for(int i = 2;i < argc;i++)
  {
    sscanf(argv[i],"%[^=]=%s",name,val);
    ftrl->SetParam(name,val);
  }
  ftrl->Run();
  if(ftrl != nullptr)
    delete ftrl;
  return 0;
}
