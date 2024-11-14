cmake_minimum_required(VERSION 3.25)
project(DFSMaker)

set(CMAKE_CXX_STANDARD 20)

add_executable(DFSMaker main.cpp)

target_link_libraries( DFSMaker arrow )
