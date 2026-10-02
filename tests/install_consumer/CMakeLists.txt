cmake_minimum_required(VERSION 3.25)
project(slick_socket_install_consumer LANGUAGES CXX)

# Deliberately sets no CMAKE_CXX_STANDARD: the C++20 requirement must come from slick::socket itself
find_package(slick-socket CONFIG REQUIRED)

add_executable(install_consumer main.cpp)
target_link_libraries(install_consumer PRIVATE slick::socket)
