CXX = g++
INC = -I. -I../../include
CFLAGS = -fPIC -fvisibility=hidden -g -O2 -Wall -Wno-deprecated
LFLAGS =  
LIBDIR = ../../lib
LIBS = util
all:
	$(CXX) -o train train.cpp adPredictor.cpp  $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))
	$(CXX) -o predict predict.cpp adPredictor.cpp $(INC) $(CFLAGS) $(LFLAGS) $(addprefix -L,$(LIBDIR)) $(addprefix -l,$(LIBS))

clean:
	rm -rf *.o *.d *.dSYM train predict
