# Graph analysis — Borůvka MST

Educational project comparing **Borůvka’s algorithm** for minimum spanning trees implemented with **SPLA** (sparse linear algebra / OpenCL) and **Apache Spark GraphX**.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Algorithms and implementations

### Borůvka

1. **SPLA** — C++ implementation on [SPLA](https://github.com/SparseLinearAlgebra/spla) (author: **Vladislav Shalnev**).
2. **Spark** — Scala implementation on **Apache Spark GraphX** (author: **Daniil Kadochkov**).

Both pipelines consume undirected graphs in **Matrix Market** (`.mtx`) format and can export benchmark timings to **CSV** for side-by-side comparison.

## Structure

```
graphs_analysis/
├── boruvka/
│   ├── boruvka_spla/     # SPLA + CMake (OpenCL/CPU)
│   ├── boruvka_spark/    # Spark GraphX + sbt
│   └── graphs/           # `.mtx` datasets, download helpers
├── experiments/
│   ├── experiment.ipynb  # download SuiteSparse graphs, stats, benchmark orchestration
│   └── results/          # CSV timing outputs (SPLA / Spark variants)
├── graphics/             # plots, flame graphs
├── slides/               # presentation sources and PDFs
└── .github/workflows/    # CI
```

## Experiments

The study compares runtime and behavior of the two Borůvka implementations on public **SuiteSparse Matrix Collection** graphs (see the notebook for the exact matrix list and download URLs).

- **`experiments/experiment.ipynb`** — fetch `.mtx` files into `boruvka/graphs`, graph statistics / preprocessing notes, and commands to run `boruvka_spla` and `boruvka_spark` benchmarks.
- **`experiments/results/`** — collected CSV results (naming reflects backend and graph, e.g. `spla-*`, `spark-*`).
- **`slides/`** — slides (`boruvka_presentation_*.md` / `.pdf`) describing SPLA, Spark, and the algorithm.

## Building and running

Each implementation lives in its own subdirectory with full build instructions, CLI options, and CSV schema:

| Component | README |
|-----------|--------|
| SPLA (CMake, OpenCL) | [`boruvka/boruvka_spla/README.md`](boruvka/boruvka_spla/README.md) |
| Spark GraphX (sbt) | [`boruvka/boruvka_spark/README.md`](boruvka/boruvka_spark/README.md) |

Shared test graphs and helpers: [`boruvka/graphs/`](boruvka/graphs/).

## License

This project is licensed under the **MIT License** — see [`LICENSE`](LICENSE).
