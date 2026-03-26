# boruvka_spla

Course project: Borůvka MST using [SPLA](https://github.com/SparseLinearAlgebra/spla). SPLA is vendored under `deps/spla` (typically as a git submodule).

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Run:

```bash
./build/boruvka_spla
```

## Tests

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Formatting

```bash
clang-format -i src/*.cpp src/*.hpp test/*.cpp
```

Check without writing: `clang-format --dry-run --Werror` on the same files.

## OpenCL

By default SPLA is built without OpenCL (`SPLA_BUILD_OPENCL=OFF` in the root `CMakeLists.txt`). For GPU acceleration, install your platform’s OpenCL dependencies and reconfigure with `-DSPLA_BUILD_OPENCL=ON`.
