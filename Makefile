CC = gcc
CFLAGS = -std=c11 -O3 -Wall -Wextra -Wpedantic -Wshadow -Wformat=2 -Wunused
LDFLAGS = -pthread
TARGET = cluster-gate
SRC_DIR = src
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = benchmarks

SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))

.PHONY: all build test clean benchmark

all: build

build: $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR)/$(TARGET): $(OBJS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

test: build
	@if [ -d "$(TEST_DIR)" ] && [ -n "$$($(MAKE) -C $(TEST_DIR) --dry-run 2>/dev/null)" ]; then \
		$(MAKE) -C $(TEST_DIR); \
	elif [ -f "$(TEST_DIR)/run_tests.sh" ]; then \
		bash $(TEST_DIR)/run_tests.sh; \
	elif [ -f "$(BUILD_DIR)/test_$(TARGET)" ]; then \
		$(BUILD_DIR)/test_$(TARGET); \
	else \
		echo "No tests configured or test directory missing. Skipping."; \
	fi

benchmark: build
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$$($(MAKE) -C $(BENCH_DIR) --dry-run 2>/dev/null)" ]; then \
		$(MAKE) -C $(BENCH_DIR); \
	elif [ -f "$(BENCH_DIR)/run_bench.sh" ]; then \
		bash $(BENCH_DIR)/run_bench.sh; \
	elif [ -f "$(BUILD_DIR)/bench_$(TARGET)" ]; then \
		$(BUILD_DIR)/bench_$(TARGET); \
	else \
		echo "No benchmarks configured or benchmark directory missing. Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)
	@if [ -d "$(TEST_DIR)" ] && [ -f "$(TEST_DIR)/Makefile" ]; then \
		$(MAKE) -C $(TEST_DIR) clean; \
	fi
	@if [ -d "$(BENCH_DIR)" ] && [ -f "$(BENCH_DIR)/Makefile" ]; then \
		$(MAKE) -C $(BENCH_DIR) clean; \
	fi