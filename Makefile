.PHONY: all test echo_binaries build clean rebuild install depend

CXX = g++-4.8.1
CC = gcc-4.8.1

OPT_FLAGS =
ifeq ($(DEBUG),1)
OPT_FLAGS += -g -DDEBUG -DWALB_DEBUG
else
OPT_FLAGS += -O2 -DNDEBUG
endif
ifeq ($(PROF),1)
OPT_FLAGS += -pg
else
endif

INCLUDES_GLOBAL = -I./cybozulib/include -I./include -I./src
INCLUDES_WALB = -I./walb/include -I./walb/tool

CFLAGS = -Wall -Wextra -D_FILE_OFFSET_BITS=64 $(OPT_FLAGS) $(INCLUDES_GLOBAL) $(INCLUDES_WALB)
CXXFLAGS = -std=c++11 -pthread $(CFLAGS)

ifeq ($(STATIC),1)
LDFLAGS = -static
LDLIBS = -Wl,--whole-archive -lpthread -Wl,--no-whole-archive
else
LDFLAGS = -Wl,-R,'$$ORIGIN'
LDLIBS = -lpthread
endif

LDLIBS_AIO = -laio
LDLIBS_SNAPPY = -lsnappy

HEADERS = $(wildcard src/*.hpp src/*.h include/*.hpp include/*.h)
SOURCES = $(wildcard src/*.cpp)
OBJECTS = $(patsubst %.cpp,%.o,$(SOURCES))
ALL_BINARIES = $(patsubst %.cpp,%,$(SOURCES))
BINARIES = $(filter-out src/test_%,$(ALL_BINARIES))
TEST_BINARIES = $(filter src/test_%,$(ALL_BINARIES)) $(patsubst %.cpp,%,$(wildcard utest/*.cpp))

all: build
build: Makefile.depends $(ALL_BINARIES)

test: $(TEST_BINARIES)
	@echo not yet implmenented

echo_binaries:
	@echo $(BINARIES)

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $< -o $(patsubst %.cpp,%.o,$<)
.c.o:
	$(CC) $(CFLAGS) -c $< -o $(patsubst %.cpp,%.o,$<)

src/%: src/%.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS) $(LDLIBS_AIO) $(LDLIBS_SNAPPY)

test/%: test/%.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f $(BINARIES) $(TEST_BINARIES) src/*.o

rebuild:
	$(MAKE) clean
	$(MAKE) all

install:
	@echo not yet implemented

depend: Makefile.depends

Makefile.depends: $(SOURCES)
	$(CXX) -MM $(SOURCES) $(CXXFLAGS) |sed -e 's|^\(.\+\.o:\)|src/\1|' > Makefile.depends

-include Makefile.depends

