cmake_minimum_required(VERSION 3.19)
project(externalsort)

set(CMAKE_BUILD_TYPE RELEASE)
set(CMAKE_CXX_STANDARD 20)

add_executable(externalsort main.cpp)
target_link_libraries(externalsort icu.lib)
