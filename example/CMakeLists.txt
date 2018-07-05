add_executable(sample main4.cpp main.cpp)

target_link_libraries(sample queue_race boost_filesystem boost_system)

set_target_properties(sample PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})