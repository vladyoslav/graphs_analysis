cmake_minimum_required(VERSION 3.15)
project(boruvka_spla LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(SPLA_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPLA_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
# CPU-only by default (CI-friendly without OpenCL). For GPU: set ON or pass -DSPLA_BUILD_OPENCL=ON
set(SPLA_BUILD_OPENCL OFF CACHE BOOL "" FORCE)

add_subdirectory(deps/spla)

add_library(boruvka_spla_lib OBJECT src/lib.cpp)
target_include_directories(boruvka_spla_lib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(boruvka_spla_lib PUBLIC spla)

add_executable(boruvka_spla src/main.cpp)
target_link_libraries(boruvka_spla PRIVATE boruvka_spla_lib)
set_target_properties(boruvka_spla PROPERTIES OUTPUT_NAME boruvka_spla)

include(CTest)
if(BUILD_TESTING)
  add_executable(boruvka_spla_test test/test_lib.cpp)
  target_link_libraries(boruvka_spla_test PRIVATE boruvka_spla_lib)
  add_test(NAME boruvka_spla_test COMMAND boruvka_spla_test)
endif()
