.DEFAULT_GOAL: build

BUILDDIR ?= $(CURDIR)/build

PLUGIN_SO := $(BUILDDIR)/libnccl-net-socket.so

SRC_FILES := \
	net_socket.cc \
	plugin_compat.cc \
	socket_compat.cc

CPPFLAGS += \
	-I. \
	-I../example/nccl

CXXFLAGS ?= -O3 -fPIC
LDFLAGS += -shared -pthread

build: $(PLUGIN_SO)

$(PLUGIN_SO): $(SRC_FILES)
	@printf "Compiling  %-35s > %s\n" "$<" "$@"
	@mkdir -p "$(BUILDDIR)"
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) -o $@ $^

clean:
	rm -rf $(BUILDDIR)
