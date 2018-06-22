cmake_minimum_required(VERSION 3.10)

project(queue_race)

set(CMAKE_CXX_STANDARD 11)

include_directories(${CMAKE_SOURCE_DIR}/include)

set(SOURCE_FILES queue_store.cpp)

add_library(queue_race SHARED ${SOURCE_FILES})

add_subdirectory(example)