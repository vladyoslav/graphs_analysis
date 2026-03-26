

# Boruvka MST — Spark GraphX

Implementation of [Borůvka's algorithm](https://en.wikipedia.org/wiki/Bor%C5%AFvka%27s_algorithm) for finding the Minimum Spanning Tree (MST) using Apache Spark GraphX.

## Prerequisites

- **Java 11**
- **sbt**

### Install Java 11

```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

### Install sbt

```bash
curl -s "https://get.sdkman.io" | bash
# restart terminal
sdk install sbt
```


## Usage

All interaction is through `Makefile`.

### Install all project dependencies

sbt downloads all dependencies (Spark, GraphX, ScalaTest) automatically on first build. To download dependencies and verify the project compiles without errors:

```bash
make build
```

### Run benchmark

```bash
make run GRAPH=/path/to/graph.mtx
```

#### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `GRAPH`   | **yes**  | —       | Path to input graph in Matrix Market (.mtx) format |
| `CSV`     | no       | —       | Path to output CSV file for results. If not then the results will be only in console |
| `RUNS`    | no       | 5       | Number of benchmark runs (must be a positive integer) |
| `CORES`   | no       | * (all) | Number of CPU cores to use (must be a positive integer or `*`) |

#### Examples

```bash
# Minimal: 5 runs, all cores, no CSV output
make run GRAPH=/path/to/graph.mtx

# Save results to CSV
make run GRAPH=/path/to/graph.mtx CSV=/path/to/results.csv

# Custom number of runs and cores
make run GRAPH=/path/to/graph.mtx CSV=/path/to/results.csv RUNS=10 CORES=4
```

### Run tests

```bash
make test
```

### Clean build artifacts

```bash
make clean
```

## Input format

[Matrix Market (.mtx)](https://math.nist.gov/MatrixMarket/formats.html) coordinate format. Both `real` (weighted) and `pattern` (unweighted) graphs are supported. For `pattern` graphs, deterministic weights are generated automatically.

Example:

```
%%MatrixMarket matrix coordinate real symmetric
%
5 5 6
2 1 1.0
3 1 3.0
3 2 2.0
4 3 5.0
5 3 6.0
5 4 4.0
```

## Output

### Console output

Each run prints human-readable results:

```
Warmup done. MST weight: 12.0
  Run 1/5: 1234.56 ms
  Run 2/5: 1100.23 ms
  ...

=== RESULTS ===
graph:       test_graph.mtx
vertices:    5
edges:       12
cores:       8
runs:        5
mst_weight:  12.0
mst_edges:   4
iterations:  2
mean_time:   1150.34 ms
std_dev:     55.12 ms
memory_mb:   45.3
```

### CSV output

If `CSV` parameter is provided, results are appended to the specified file. The header is written only when the file is created.

```
library,graph,vertices,edges,cores,runs,mst_weight,mst_edges,iterations,mean_time_ms,std_dev_ms,memory_mb
spark,test_graph.mtx,5,12,8,5,12.0,4,2,1150.34,55.12,45.3
```

#### CSV columns

| Column | Description |
|--------|-------------|
| `library` | Library name (`spark`) |
| `graph` | Input file name |
| `vertices` | Number of vertices in the graph |
| `edges` | Number of directed edges (2× undirected edges) |
| `cores` | Number of CPU cores used |
| `runs` | Number of benchmark runs |
| `mst_weight` | Total weight of the MST |
| `mst_edges` | Number of edges in the MST (should be V-1) |
| `iterations` | Number of Boruvka iterations |
| `mean_time_ms` | Mean algorithm execution time in milliseconds |
| `std_dev_ms` | Standard deviation of execution time |
| `memory_mb` | Approximate JVM memory usage in megabytes |

## Error handling

| Error | When |
|-------|------|
| `Error: --graph is required` | No `GRAPH` parameter provided |
| `Error: file not found: ...` | Graph file does not exist |
| `Error: not a file: ...` | Path points to a directory |
| `Error: directory does not exist: ...` | Parent directory for CSV output does not exist |
| `Error: --runs must be a positive integer` | `RUNS` is not a valid positive number |
| `Error: --cores must be a positive integer or *` | `CORES` is not a valid positive number or `*` |

> **Note:** The CSV output directory must be created manually before running. The program creates the CSV file itself but does not create directories.