.DEFAULT_GOAL: build

BUILDDIR ?= $(CURDIR)/build
TEST_BIN := $(BUILDDIR)/nccl-net-plugin-test

SRC_FILES := net_plugin_so_test.cc

CPPFLAGS += -I../example/nccl
CXXFLAGS ?= -O3 -fPIC

build: $(TEST_BIN)

$(TEST_BIN): $(SRC_FILES)
	@printf "Compiling  %-35s > %s\n" "$<" "$@"
	@mkdir -p "$(BUILDDIR)"
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LDLIBS)

clean:
	rm -rf $(BUILDDIR)
