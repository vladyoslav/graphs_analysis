cmake_minimum_required(VERSION 3.15)
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()
project(boruvka_spla LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(SPLA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPLA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
# AccCsr requires OpenCL support in SPLA. Turn OFF for CPU-only: -DSPLA_BUILD_OPENCL=OFF (then use CpuCsr in code).
set(SPLA_BUILD_OPENCL ON CACHE BOOL "" FORCE)

add_subdirectory(deps/spla)

include(FetchContent)
FetchContent_Declare(
  cxxopts
  URL https://github.com/jarro2783/cxxopts/archive/refs/tags/v3.3.1.tar.gz
)
FetchContent_MakeAvailable(cxxopts)

add_library(boruvka_spla_lib OBJECT src/lib.cpp src/boruvka_classic.cpp)
target_include_directories(boruvka_spla_lib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(boruvka_spla_lib PUBLIC spla)

add_executable(boruvka_spla src/main.cpp)
target_link_libraries(boruvka_spla PRIVATE boruvka_spla_lib cxxopts::cxxopts)
set_target_properties(boruvka_spla PROPERTIES OUTPUT_NAME boruvka_spla)

include(CTest)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/52eb8108c5bdec04579160ae17225d66034bd723.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

if(BUILD_TESTING)
  add_executable(lib_test test/lib_test.cpp)
  target_link_libraries(lib_test PRIVATE boruvka_spla_lib GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(lib_test)
endif()
