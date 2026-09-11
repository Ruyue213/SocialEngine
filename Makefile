CXX ?= clang++
CC ?= clang
CXXFLAGS ?= -std=c++17 -O2 -g -Wall -Wextra -Isrc -Ithird_party/hiredis
CFLAGS ?= -std=c11 -O2 -w -Ithird_party/hiredis
LDFLAGS ?= -lpthread

BUILD := build
BIN := bin

ENGINE_SRC := $(wildcard src/common/*.cpp src/memory/*.cpp src/storage/*.cpp \
                src/concurrency/*.cpp src/cache/*.cpp src/graph/*.cpp src/baseline/*.cpp)
ENGINE_OBJ := $(patsubst src/%.cpp,$(BUILD)/obj/%.o,$(ENGINE_SRC))

HIREDIS_SRC := third_party/hiredis/alloc.c third_party/hiredis/hiredis.c \
               third_party/hiredis/net.c third_party/hiredis/read.c \
               third_party/hiredis/sds.c third_party/hiredis/sockcompat.c \
               third_party/hiredis/async.c third_party/hiredis/dict.c
HIREDIS_OBJ := $(patsubst %.c,$(BUILD)/%.o,$(HIREDIS_SRC))

BENCH_OBJ := $(BUILD)/obj/bench/bench_main.o
TEST_OBJ := $(BUILD)/obj/tests/test_main.o

all: $(BIN)/bench $(BIN)/tests

$(BUILD)/obj/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD)/obj/tests/test_main.o: tests/test_main.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN)/bench: $(BENCH_OBJ) $(ENGINE_OBJ) $(HIREDIS_OBJ)
	@mkdir -p $(BIN)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

$(BIN)/tests: $(TEST_OBJ) $(ENGINE_OBJ) $(HIREDIS_OBJ)
	@mkdir -p $(BIN)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

asan:
	$(MAKE) CXXFLAGS="-std=c++17 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -Isrc -Ithird_party/hiredis"

debug:
	$(MAKE) CXXFLAGS="-std=c++17 -O0 -g -Wall -Wextra -Isrc -Ithird_party/hiredis"

test: $(BIN)/tests
	./$(BIN)/tests

bench: $(BIN)/bench
	./$(BIN)/bench

clean:
	rm -rf $(BUILD) $(BIN)

.PHONY: all asan debug test bench clean
