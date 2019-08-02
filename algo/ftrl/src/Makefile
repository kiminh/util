CXX = g++
INC = -I. -I../include
CFLAGS = -std=c++0x -fPIC -fvisibility=hidden -O2 -Wall -Wno-deprecated
LFLAGS = 
LIBDIR = ../lib
LIBS = 
BIN = ../bin
all:
	$(CXX) -o $(BIN)/train train.cpp ftrl.cpp conf_util.cpp str_util.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))
	$(CXX) -o $(BIN)/predict predict.cpp ftrl.cpp conf_util.cpp str_util.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))
	$(CXX) -o $(BIN)/shuffle shuffle.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))
	$(CXX) -o $(BIN)/text2bin text2bin.cpp 
	$(CXX) -o $(BIN)/bin2text bin2text.cpp 


clean:
	rm -rf *.o *.dSYM $(BIN)/train $(BIN)/predict
