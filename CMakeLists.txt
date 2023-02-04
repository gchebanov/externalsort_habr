cmake_minimum_required(VERSION 3.19)
project(externalsort)

set(CMAKE_BUILD_TYPE RELEASE)
set(CMAKE_CXX_STANDARD 20)

if (MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
#    add_compile_options("-march=native -mtune=native")
endif()


add_executable(externalsort main.cpp)
target_link_libraries(externalsort icu.lib)
