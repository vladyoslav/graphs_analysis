# boruvka_spla

Course project: **Borůvka’s algorithm** for a minimum spanning tree using [SPLA](https://github.com/SparseLinearAlgebra/spla) (sparse linear algebra, OpenCL/CPU). SPLA lives under `deps/spla` and is usually a **git submodule**.

## Clone with submodules

Prefer a recursive clone so `deps/spla` (and any nested submodules) are populated:

```bash
git clone --recurse-submodules <repository-url>
cd boruvka_spla   # or your project path
```

If you already cloned without submodules:

```bash
git submodule update --init --recursive
```

Nested submodules (inside `deps/spla`) are initialized by `--recursive` / `--init --recursive`.

## Overview

- **`load_graph`**: Matrix Market (square matrix, lines `u v w`, 1-based vertices).
- **`boruvka_mst`**: SPLA `exec_m_reduce_by_row` + DSU on the CPU; edge weight and neighbor are packed into one `uint32` (see `lib.hpp`).

Build dependencies **cxxopts** and **GoogleTest** are fetched via CMake `FetchContent`.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Binary: `build/boruvka_spla`.

## Benchmark

`--mtxpath` (`-m`) and `--out` (`-o`) are required. See `--help` for all flags.

```bash
./build/boruvka_spla --mtxpath ../datasets/test_graph.mtx --out result.csv
```

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--mtxpath` | `-m` | Path to `.mtx` graph | required |
| `--out` | `-o` | Output **CSV** path | required |
| `--niters` | `-n` | Number of timed runs | `10` |
| `--warmup` | `-w` | Warmup runs (not timed) | `3` |
| `--platform` | `-p` | SPLA platform index | `0` |
| `--device` | `-d` | SPLA device index | `0` |
| `--help` | `-h` | Print help | — |

Flow: load graph → one validation MST run → `warmup` × `boruvka_mst` → `niters` × timed `boruvka_mst` using `spla::Timer` (mean and sample std. dev. in ms). The CSV has a header row plus one data row: `library`, `graph`, `vertices`, `edges` (directed nonzeros), `cores`, `runs`, `mst_weight`, `mst_edges`, `iterations` (same as the number of timed runs), `mean_time_ms`, `std_dev_ms`, `memory_mb` (currently `0`).

## Tests

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Test binary: **`lib_test`** (`test/lib_test.cpp`).

## Formatting

```bash
clang-format -i src/*.cpp src/*.hpp test/*.cpp
```

Check only: `clang-format --dry-run --Werror` on the same paths.

## OpenCL

Root `CMakeLists.txt` defaults to **`SPLA_BUILD_OPENCL=ON`** (matches `AccCsr` in the current code). For CPU-only, configure with `-DSPLA_BUILD_OPENCL=OFF` and switch matrix format in code (e.g. `CpuCsr` instead of `AccCsr`), or accelerator-backed ops may fail.

SPLA C++ API reference: [docs](https://sparselinearalgebra.github.io/spla/docs-cpp/group__spla.html).
