cmake_minimum_required(VERSION 3.16)
project(impulse_ring_cpp CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra -O2)

find_package(Threads REQUIRED)

# Reuse the native C connector core (compiled directly, no protocol duplication).
add_library(impulse_ring_c STATIC ../c/src/impulse_ring.c)
target_include_directories(impulse_ring_c PUBLIC ../c/include)
target_compile_definitions(impulse_ring_c PRIVATE _GNU_SOURCE)
target_link_libraries(impulse_ring_c PUBLIC Threads::Threads)

add_executable(irpp_demo examples/demo.cpp)
target_include_directories(irpp_demo PRIVATE include)
target_link_libraries(irpp_demo PRIVATE impulse_ring_c)

enable_testing()
add_executable(irpp_test_e2e tests/test_e2e.cpp)
target_include_directories(irpp_test_e2e PRIVATE include)
target_link_libraries(irpp_test_e2e PRIVATE impulse_ring_c)
add_test(NAME e2e COMMAND irpp_test_e2e)
