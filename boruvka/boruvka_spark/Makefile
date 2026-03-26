GRAPH ?=
CSV ?=
RUNS ?= 5
CORES ?= *

# Build the arguments string dynamically
ARGS = --graph=$(GRAPH) --runs=$(RUNS) --cores=$(CORES)
ifdef CSV
ARGS += --csv=$(CSV)
endif

.PHONY: run test clean

run:
ifndef GRAPH
	$(error GRAPH is required. Usage: make run GRAPH=/path/to/graph.mtx [CSV=...] [RUNS=5] [CORES=*])
endif
	sbt "run $(ARGS)"

test:
	sbt test

clean:
	sbt clean
	