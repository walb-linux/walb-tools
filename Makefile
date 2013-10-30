.PHONY: all test test_all echo_binaries build clean rebuild install

CXX = g++-4.8.1
CC = gcc-4.8.1

OPT_FLAGS =
ifeq ($(DEBUG),1)
OPT_FLAGS += -g -rdynamic -DDEBUG -DWALB_DEBUG
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
LDLIBS_COMPRESS = -lsnappy -llzma -lz

HEADERS = $(wildcard src/*.hpp src/*.h include/*.hpp include/*.h utest/*.hpp)
BIN_SOURCES = $(wildcard binsrc/*.cpp)
OTHER_SOURCES = $(wildcard src/*.cpp)
TEST_SOURCES = $(wildcard utest/*.cpp)
SOURCES = $(OTHER_SOURCES) $(BIN_SOURCES) $(TEST_SOURCES)
DEPENDS = $(patsubst %.cpp,%.depend,$(SOURCES))
BINARIES = $(patsubst %.cpp,%,$(BIN_SOURCES))
TEST_BINARIES = $(patsubst %.cpp,%,$(TEST_SOURCES))

all: build
build: $(BINARIES)

test: $(TEST_BINARIES)
test_all: $(TEST_BINARIES)
	@for t in $(TEST_BINARIES); do \
	    ./$$t; \
	done 2>&1 |tee test.log |grep ^ctest:name

echo_binaries:
	@echo $(BINARIES)

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $< -o $(patsubst %.cpp,%.o,$<)
.c.o:
	$(CC) $(CFLAGS) -c $< -o $(patsubst %.cpp,%.o,$<)

binsrc/client: binsrc/client.o src/compressor.o src/MurmurHash3.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< \
src/compressor.o src/MurmurHash3.o $(LDLIBS) $(LDLIBS_AIO) $(LDLIBS_COMPRESS)
binsrc/server: binsrc/server.o src/compressor.o src/MurmurHash3.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< \
src/compressor.o src/MurmurHash3.o $(LDLIBS) $(LDLIBS_AIO) $(LDLIBS_COMPRESS)
binsrc/%: binsrc/%.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS) $(LDLIBS_AIO) $(LDLIBS_COMPRESS)

utest/compressor_test: utest/compressor_test.o src/compressor.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS) src/compressor.o $(LDLIBS_COMPRESS)
utest/wlog_compressor_test: utest/wlog_compressor_test.o src/compressor.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS) src/compressor.o $(LDLIBS_COMPRESS)
utest/hash_test: utest/hash_test.o src/MurmurHash3.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS) src/MurmurHash3.o
utest/%: utest/%.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f $(BINARIES) $(TEST_BINARIES) src/*.o binsrc/*.o utest/*.o

cleandep:
	rm -f src/*.depend binsrc/*.depend utest/*.depend

rebuild:
	$(MAKE) clean
	$(MAKE) all

install:
	@echo not yet implemented

binsrc/%.depend: binsrc/%.cpp
	$(CXX) -MM $< $(CXXFLAGS) |sed -e 's|^\(.\+\.o:\)|binsrc/\1|' > $@
src/%.depend: src/%.cpp
	$(CXX) -MM $< $(CXXFLAGS) |sed -e 's|^\(.\+\.o:\)|src/\1|' > $@
utest/%.depend: utest/%.cpp
	$(CXX) -MM $< $(CXXFLAGS) |sed -e 's|^\(.\+\.o:\)|utest/\1|' > $@

ifneq "$(MAKECMDGOALS)" "clean"
-include $(DEPENDS)
endif
