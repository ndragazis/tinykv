COMPILER = g++
CFLAGS = -g -I.
MODE = release
SOURCES := $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)
TARGET = app
SEASTAR_LIB = /opt/seastar/build/$(MODE)/libseastar.a

all: $(TARGET)

%.o: %.cc
	$(COMPILER) $(CFLAGS) $(shell pkg-config --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) -c $< -o $@

$(TARGET): $(SEASTAR_LIB) $(OBJECTS)
	$(COMPILER) $(OBJECTS) $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) -o $(TARGET)

$(SEASTAR_LIB):
	cd /opt/seastar && ./configure.py --mode="$(MODE)" --disable-dpdk --disable-hwloc --cflags="$(CFLAGS)" --compiler="$(COMPILER)"
	ninja -C /opt/seastar/build/$(MODE) libseastar.a
