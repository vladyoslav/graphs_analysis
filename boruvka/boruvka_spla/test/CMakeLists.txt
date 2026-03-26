# Parent project does not export its library target, so this CML implicitly
# depends on being added from it, i.e. the testing is done only from the build
# tree and is not feasible from an install location

project(boruvka_splaTests LANGUAGES CXX)

# ---- Tests ----

add_executable(boruvka_spla_test src/boruvka_spla_test.cpp)
target_link_libraries(boruvka_spla_test PRIVATE boruvka_spla_lib)
target_compile_features(boruvka_spla_test PRIVATE cxx_std_20)

add_test(NAME boruvka_spla_test COMMAND boruvka_spla_test)

# ---- End-of-file commands ----

add_folders(Test)
