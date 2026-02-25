cmake_minimum_required(VERSION 3.16)
project(HeliosDB VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

include_directories(include)

add_library(heliosdb
    src/db.cpp
    src/wal.cpp
    src/sstable.cpp
)

add_executable(main src/main.cpp)
target_link_libraries(main heliosdb)

enable_testing()
add_executable(basic_test tests/basic_test.cpp)
target_link_libraries(basic_test heliosdb)
add_test(NAME BasicTest COMMAND basic_test)
# ---- Benchmarks ----
find_package(benchmark REQUIRED)

add_executable(lsm_bench benchmarks/lsm_bench.cpp)
target_link_libraries(lsm_bench heliosdb benchmark::benchmark)
