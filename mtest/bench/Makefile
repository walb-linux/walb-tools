CXX = g++-6.3

INCLUDES = -I../../walb/include -I../../cybozulib/include -I../../include -I../../src
CFLAGS = -O2 -ftree-vectorize -g -DNDEBUG $(INCLUDES)
CXXFLAGS = -std=c++11 -pthread $(CFLAGS) 

bench_csum: bench_csum.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< -MMD -MP


clean:
	rm -f *.o bench_csum

ALL_SRC = bench_csum.cpp

DEPEND_FILE=$(ALL_SRC:.cpp=.d)
-include $(DEPEND_FILE)

# don't remove these files automatically
.SECONDARY: $(ALL_SRC:.cpp=.o)

