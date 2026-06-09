cmake_minimum_required(VERSION 3.16)
project(impulse_ring_c C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra -O2)
# Needed for syscall()/futex; defined here (not in the source) to avoid defining
# a reserved identifier in the translation unit.
add_compile_definitions(_GNU_SOURCE)

find_package(Threads REQUIRED)

add_library(impulse_ring STATIC src/impulse_ring.c)
target_include_directories(impulse_ring PUBLIC include)
target_link_libraries(impulse_ring PUBLIC Threads::Threads)

add_library(impulse_ring_shared SHARED src/impulse_ring.c)
set_target_properties(impulse_ring_shared PROPERTIES OUTPUT_NAME impulse_ring)
target_include_directories(impulse_ring_shared PUBLIC include)
target_link_libraries(impulse_ring_shared PUBLIC Threads::Threads)

add_executable(ir_demo examples/demo.c)
target_link_libraries(ir_demo PRIVATE impulse_ring)

add_executable(ir_xlang tests/xlang_client.c)
target_link_libraries(ir_xlang PRIVATE impulse_ring)

enable_testing()
add_executable(ir_test_e2e tests/test_e2e.c)
target_link_libraries(ir_test_e2e PRIVATE impulse_ring)
add_test(NAME e2e COMMAND ir_test_e2e)
