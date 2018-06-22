cmake_minimum_required(VERSION 3.0)

project(queue_race)

set(CMAKE_CXX_STANDARD 11)

include_directories(${CMAKE_SOURCE_DIR}/include)

set(SOURCE_FILES queue_store.cpp)

add_library(queue_race SHARED ${SOURCE_FILES})

# In case you need boost::filesystem
#target_link_libraries(queue_race boost_filesystem boost_system)

# In case you need lz4
#target_link_libraries(queue_race lz4)

# In case you need lzma
#target_link_libraries(queue_race lzma)

# In case you need snappy
#target_link_libraries(queue_race snappy)

# In case you need lz
#target_link_libraries(queue_race z)

option(BUILD_EXAMPLE OFF)
if(${BUILD_EXAMPLE})
    add_subdirectory(example)
endif()

install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/
        DESTINATION include)
install(TARGETS queue_race
        LIBRARY DESTINATION lib)