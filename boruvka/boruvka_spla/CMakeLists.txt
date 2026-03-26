cmake_minimum_required(VERSION 3.14)

include(cmake/prelude.cmake)

project(
    boruvka_spla
    VERSION 0.1.0
    DESCRIPTION "Short description"
    HOMEPAGE_URL "https://example.com/"
    LANGUAGES CXX
)

include(cmake/project-is-top-level.cmake)
include(cmake/variables.cmake)

# ---- Declare library ----

add_library(
    boruvka_spla_lib OBJECT
    src/lib.cpp
)

target_include_directories(
    boruvka_spla_lib ${warning_guard}
    PUBLIC
    "\$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)

target_compile_features(boruvka_spla_lib PUBLIC cxx_std_20)

# ---- Declare executable ----

add_executable(boruvka_spla_exe src/main.cpp)
add_executable(boruvka_spla::exe ALIAS boruvka_spla_exe)

set_property(TARGET boruvka_spla_exe PROPERTY OUTPUT_NAME boruvka_spla)

target_compile_features(boruvka_spla_exe PRIVATE cxx_std_20)

target_link_libraries(boruvka_spla_exe PRIVATE boruvka_spla_lib)

# ---- Install rules ----

if(NOT CMAKE_SKIP_INSTALL_RULES)
  include(cmake/install-rules.cmake)
endif()

# ---- Developer mode ----

if(NOT boruvka_spla_DEVELOPER_MODE)
  return()
elseif(NOT PROJECT_IS_TOP_LEVEL)
  message(
      AUTHOR_WARNING
      "Developer mode is intended for developers of boruvka_spla"
  )
endif()

include(cmake/dev-mode.cmake)
