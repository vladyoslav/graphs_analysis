# Boruvka MST — Spark GraphX

Implementation of [Borůvka's algorithm](https://en.wikipedia.org/wiki/Bor%C5%AFvka%27s_algorithm) for finding a minimum spanning tree / minimum spanning forest using Apache Spark GraphX.

The implementation targets **undirected graphs** stored in Matrix Market (`.mtx`) format and supports both:

- **weighted graphs** (`real`)
- **unweighted graphs** (`pattern`)

For unweighted graphs, deterministic synthetic weights are assigned as:

\[
w(u, v) = \min(u, v)
\]

where `u` and `v` are endpoint vertex IDs.

---

## Project status

This version includes:

- Spark/GraphX-based Borůvka implementation
- correctness validation mode
- benchmark mode
- optional debug output
- optional step-by-step profiling
- optional memory profiling
- CSV export

---

## Prerequisites

- **Java 11**
- **sbt**
- **GNU Make** *(optional, only if you want to use the Makefile wrapper)*

---

## Installation

### Linux / WSL

#### Install Java 11

```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

#### Install sbt

Option 1: via SDKMAN

```bash
curl -s "https://get.sdkman.io" | bash
# restart terminal
sdk install sbt
```

Option 2: use the official sbt installation instructions:  
https://www.scala-sbt.org/download/

#### Verify installation

```bash
java -version
javac -version
sbt --version
```

---

### Windows

Install:

- **Java 11** (recommended: Temurin / Eclipse Adoptium)
- **sbt**
- **Git** *(optional, if cloning the repository)*

Recommended Java distribution:  
https://adoptium.net/

Recommended sbt installer:  
https://www.scala-sbt.org/download/

#### Verify installation in PowerShell

```powershell
java -version
javac -version
sbt --version
```

---

## Build

To download dependencies and compile the project:

### Using sbt

```bash
sbt compile
```

### Using Makefile

```bash
make build
```

---

# Usage

There are **two supported ways** to run the project:

1. **directly via `sbt`** — recommended cross-platform interface
2. **via `Makefile`** — convenient wrapper mainly for Linux/WSL

---

## 1. Running via sbt

### Basic command

```bash
sbt "run --graph=/path/to/graph.mtx"
```

### Full command syntax

```bash
sbt "run --graph=/path/to/graph.mtx --runs=3 --warmup=3 --cores=4 --csv=/path/to/results.csv --debug --checks --profile --mem"
```

### Supported command-line parameters

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--graph=<path>` | **yes** | — | Path to input graph in Matrix Market (`.mtx`) format |
| `--csv=<path>` | no | — | Append benchmark results to CSV file |
| `--runs=<n>` | no | `5` | Number of benchmark runs |
| `--warmup=<n>` | no | `3` | Number of warmup runs |
| `--cores=<n or *>` | no | `*` | Number of local CPU cores for Spark |
| `--debug` | no | off | Enable per-iteration algorithm log |
| `--checks` | no | off | Enable final correctness validation |
| `--profile` | no | off | Enable per-step profiling inside Borůvka |
| `--mem` | no | off | Enable memory profiling |

### Examples

#### Minimal run

```bash
sbt "run --graph=/path/to/minnesota.mtx"
```

#### Save results to CSV

```bash
sbt "run --graph=/path/to/minnesota.mtx --csv=/path/to/results.csv"
```

#### Benchmark run with custom parameters

```bash
sbt "run --graph=/path/to/minnesota.mtx --runs=3 --warmup=3 --cores=4"
```

#### Correctness validation

```bash
sbt "run --graph=/path/to/delaunay_n13.mtx --runs=1 --warmup=0 --cores=4 --checks"
```

#### Debug / profiling

```bash
sbt "run --graph=/path/to/minnesota.mtx --runs=1 --warmup=0 --cores=4 --debug"
sbt "run --graph=/path/to/minnesota.mtx --runs=1 --warmup=0 --cores=4 --profile"
sbt "run --graph=/path/to/minnesota.mtx --runs=1 --warmup=0 --cores=4 --mem"
```

---

## 2. Running via Makefile

### Basic command

```bash
make run GRAPH=/path/to/graph.mtx
```

### Supported Make variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GRAPH` | **yes** | — | Path to input graph |
| `CSV` | no | — | Path to output CSV |
| `RUNS` | no | `5` | Number of benchmark runs |
| `WARMUP` | no | `3` | Number of warmup runs |
| `CORES` | no | `*` | Number of local CPU cores |
| `DEBUG` | no | `0` | Set to `1` to enable `--debug` |
| `CHECKS` | no | `0` | Set to `1` to enable `--checks` |
| `PROFILE` | no | `0` | Set to `1` to enable `--profile` |
| `MEM` | no | `0` | Set to `1` to enable `--mem` |

### Examples

#### Minimal run

```bash
make run GRAPH=/path/to/minnesota.mtx
```

#### Benchmark run with CSV

```bash
make run GRAPH=/path/to/minnesota.mtx CSV=/path/to/results.csv RUNS=3 WARMUP=3 CORES=4
```

#### Correctness validation

```bash
make run GRAPH=/path/to/delaunay_n13.mtx RUNS=1 WARMUP=0 CORES=4 CHECKS=1
```

#### Debug / profile / memory

```bash
make run GRAPH=/path/to/minnesota.mtx RUNS=1 WARMUP=0 CORES=4 DEBUG=1
make run GRAPH=/path/to/minnesota.mtx RUNS=1 WARMUP=0 CORES=4 PROFILE=1
make run GRAPH=/path/to/minnesota.mtx RUNS=1 WARMUP=0 CORES=4 MEM=1
```

---

## Running tests

### Using sbt

```bash
sbt test
```

### Using Makefile

```bash
make test
```

---

## Cleaning build artifacts

### Using sbt

```bash
sbt clean
```

### Using Makefile

```bash
make clean
```

---

# Input format

The program expects graphs in [Matrix Market (`.mtx`)](https://math.nist.gov/MatrixMarket/formats.html) coordinate format.

Supported variants:

- `real symmetric` — weighted undirected graph
- `pattern symmetric` — unweighted undirected graph
- non-symmetric coordinate graphs are also accepted, but the algorithm interprets the graph exactly as given by the edge list

### Example weighted graph

```text
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

### Example unweighted graph

```text
%%MatrixMarket matrix coordinate pattern symmetric
%
5 5 6
2 1
3 1
3 2
4 3
5 3
5 4
```

For `pattern` graphs, weights are generated automatically as:

```text
weight(u, v) = min(u, v)
```

---

# Graph representation

The algorithm solves the problem on **undirected graphs**.

For symmetric Matrix Market inputs, the loader keeps **one physical edge per undirected connection** instead of explicitly storing both directions. This reduces memory usage and improves performance while preserving undirected behavior in GraphX message passing.

---

# Output

## Console output

A normal benchmark run prints:

- input graph metadata
- warmup information
- benchmark run times
- final result summary
- optional correctness block (when `--checks` is enabled)
- optional profiling / memory output (when enabled)

### Example benchmark output

```text
=== INPUT ===
graph:              minnesota.mtx
vertices:           2642
edges:              3303
components:         skipped
expected_mst_edges: skipped
cores:              4
warmup_runs:        3
benchmark_runs:     3
debug:              false
checks:             false
profile:            false
mem:                false

Warmup 1/3 done. weight=3432333.0, edges=2640, iterations=4
Warmup 2/3 done. weight=3432333.0, edges=2640, iterations=4
Warmup 3/3 done. weight=3432333.0, edges=2640, iterations=4

Run 1/3: 891.01 ms
Run 2/3: 885.66 ms
Run 3/3: 673.71 ms

=== RESULTS ===
graph:       minnesota.mtx
vertices:    2642
edges:       3303
cores:       4
mst_weight:  3432333.0
mst_edges:   2640
iterations:  4
```

---

## Correctness output (`--checks`)

When `--checks` is enabled, the program additionally prints:

```text
=== RESULT CHECK ===
input_vertices:          2642
result_vertices_covered: 2642
vertex_count_correct:    true
input_components:        2
result_components:       2
component_count_correct: true
expected_result_edges:   2640
actual_result_edges:     2640
edge_count_correct:      true
```

### Notes

- `expected_result_edges = vertices - components`
- for disconnected graphs, the algorithm computes a **minimum spanning forest**
- on graphs containing isolated vertices, `vertex_count_correct` may be `false` if an isolated vertex is present in metadata but absent from all edges

---

## CSV output

If `--csv` / `CSV=...` is provided, each benchmark run is appended as a separate CSV row.

### Header

```text
library,graph,vertices,edges,cores,mst_weight,mst_edges,time_ms
```

### Example rows

```text
spark,minnesota.mtx,2642,3303,4,3432333.0,2640,891.01
spark,minnesota.mtx,2642,3303,4,3432333.0,2640,885.66
spark,minnesota.mtx,2642,3303,4,3432333.0,2640,673.71
```

### CSV columns

| Column | Description |
|--------|-------------|
| `library` | Library name (`spark`) |
| `graph` | Input file name |
| `vertices` | Number of vertices from Matrix Market metadata |
| `edges` | Number of undirected edges from Matrix Market metadata |
| `cores` | Number of CPU cores used |
| `mst_weight` | Total weight of the resulting MST/MSF |
| `mst_edges` | Number of edges in the resulting MST/MSF |
| `time_ms` | Execution time of `Boruvka.run(...)` in milliseconds |

---

# Recommended modes

## Correctness validation

Use this on representative graphs:

```bash
--runs=1 --warmup=0 --checks
```

## Benchmark mode

Use this for timing measurements.

For smaller and medium-size graphs:

```bash
--runs=3 --warmup=3
```

For larger graphs:

```bash
--runs=3 --warmup=1
```

---

# Error handling

| Error | Meaning |
|------|---------|
| `Error: --graph is required` | No input graph path provided |
| `Error: file not found: ...` | Graph file does not exist |
| `Error: not a file: ...` | Provided path is not a regular file |
| `Error: directory does not exist: ...` | Parent directory of CSV output does not exist |
| `Error: --runs must be a positive integer` | Invalid `runs` value |
| `Error: --warmup must be a non-negative integer` | Invalid `warmup` value |
| `Error: --cores must be a positive integer or *` | Invalid `cores` value |
| `Could not find Matrix Market size line` | Could not extract graph metadata from `.mtx` file |

---

# Notes on profiling

## `--debug`

Prints high-level iteration progress.

## `--profile`

Prints detailed per-step timings inside one Borůvka iteration.

## `--mem`

Prints JVM heap usage and estimated sizes of selected local structures. Intended for diagnostics only.

These modes are useful for debugging and performance investigation, but they are not needed for normal benchmark runs.

---

# Notes on portability

The recommended cross-platform interface is:

```bash
sbt "run ..."
```

The `Makefile` is mainly a convenience wrapper for Linux/WSL users.

---

# Notes on scale

The implementation was validated and benchmarked successfully on all selected project graphs, including larger inputs such as:

- `usroads-48`
- `delaunay_n15`
- `email-Enron`
- `kron_g500-logn21`

An additional stress test on `road-usa` was attempted, but this graph exceeded the practical local-memory limits of the current Spark/GraphX setup on the available machine.

---
