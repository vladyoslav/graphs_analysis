GRAPH ?=
CSV ?=
RUNS ?= 5
WARMUP ?= 3
CORES ?= *
DEBUG ?= 0
CHECKS ?= 0

# Build the arguments string dynamically
ARGS = --graph=$(GRAPH) --runs=$(RUNS) --warmup=$(WARMUP) --cores=$(CORES)

ifneq ($(strip $(CSV)),)
ARGS += --csv=$(CSV)
endif

ifeq ($(DEBUG),1)
ARGS += --debug
endif

ifeq ($(CHECKS),1)
ARGS += --checks
endif

.PHONY: run test clean build

build:
	sbt compile

run:
ifndef GRAPH
	$(error GRAPH is required. Usage: make run GRAPH=/path/to/graph.mtx [CSV=...] [RUNS=5] [WARMUP=3] [CORES=*] [DEBUG=1])
endif
	sbt "run $(ARGS)"

test:
	sbt test

clean:
	sbt clean
	