cmake_minimum_required(VERSION 2.8)
project(external_sort)

##################
# Compiler setting
##################
# Use C++ 11
set(CMAKE_CXX_FLAGS "-Wall -O3 -std=c++11")
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")

########################
# Build executable files
########################
set(EXECUTABLE_OUTPUT_PATH "${PROJECT_BINARY_DIR}/bin")
add_executable(external_sort main.cpp ExternalSort.cpp ExternalSort.h MinHeap.cpp MinHeap.h)

########################
# Build test program
########################
set(EXECUTABLE_OUTPUT_PATH "${PROJECT_BINARY_DIR}/bin")
add_executable(generate_text_file test/generate_text_file.cpp)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(external_sort PRIVATE Threads::Threads)

