# Tests CMakeLists.txt

find_package(GTest CONFIG QUIET)

if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.17.0
    )
    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
endif()

# Include Google Test
include(GoogleTest)

# Network Tests
add_executable(tests
    tcp_server_tests.cpp
    tcp_client_tests.cpp
    integration_tests.cpp
    multicast_sender_tests.cpp
    multicast_receiver_tests.cpp
    build_config_tests.cpp
)

target_link_libraries(tests
    PRIVATE
    GTest::gtest_main
    slick::socket
)

target_include_directories(tests
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

slick_socket_enable_warnings(tests)
# std::getenv is only read, so MSVC's _dupenv_s suggestion does not apply
target_compile_definitions(tests PRIVATE $<$<CXX_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS>)
# The configuration actually being compiled, which build_config_tests.cpp checks the build-type macros against
target_compile_definitions(tests PRIVATE SLICK_SOCKET_TEST_DEBUG_CONFIG=$<IF:$<CONFIG:Debug>,1,0>)

# Compiles every public header alone in its own translation unit, so a header that relies on another
# header's incidental includes breaks this build instead of a consumer's
file(GLOB slick_socket_public_headers CONFIGURE_DEPENDS ${PROJECT_SOURCE_DIR}/include/slick/socket/*.h)
set(header_check_sources)
foreach(header IN LISTS slick_socket_public_headers)
    get_filename_component(header_name ${header} NAME)
    get_filename_component(header_stem ${header} NAME_WE)
    set(header_check_source ${CMAKE_CURRENT_BINARY_DIR}/header_check/${header_stem}.cpp)
    file(CONFIGURE OUTPUT ${header_check_source} CONTENT "#include <slick/socket/${header_name}>
")
    list(APPEND header_check_sources ${header_check_source})
endforeach()
add_library(header_check OBJECT ${header_check_sources})
target_link_libraries(header_check PRIVATE slick::socket)
slick_socket_enable_warnings(header_check)

# Discover tests with increased timeout
gtest_discover_tests(tests
    DISCOVERY_TIMEOUT 30
)

# Set test properties
set_target_properties(tests PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests
)

# Installs the package to a scratch prefix, then builds a consumer that sets no C++ standard against it,
# so usage requirements missing from the exported slick::socket target fail here rather than for users
set(SLICK_SOCKET_TEST_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/install_prefix)
add_test(NAME InstallPackage
    COMMAND ${CMAKE_COMMAND} --install ${PROJECT_BINARY_DIR} --prefix ${SLICK_SOCKET_TEST_PREFIX} --config $<CONFIG>
)
set_tests_properties(InstallPackage PROPERTIES FIXTURES_SETUP slick_socket_installed)

# Pinned to the fresh install, so another slick-socket on the search path (e.g. from vcpkg) cannot be used instead
set(consumer_options -Dslick-socket_DIR=${SLICK_SOCKET_TEST_PREFIX}/lib/cmake/slick-socket)
if(WIN32 AND NOT SLICK_SOCKET_INSTALL_WEPOLL)
    list(APPEND consumer_options -DWEPOLL_INCLUDE_DIR=${WEPOLL_INCLUDE_DIR} -DWEPOLL_LIBRARY=${WEPOLL_LIBRARY})
endif()
if(NOT CMAKE_GENERATOR MATCHES "^Visual Studio")
    list(APPEND consumer_options -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_BUILD_TYPE=$<CONFIG>)
endif()

set(consumer_generator_args --build-generator ${CMAKE_GENERATOR})
if(CMAKE_GENERATOR_PLATFORM)
    list(APPEND consumer_generator_args --build-generator-platform ${CMAKE_GENERATOR_PLATFORM})
endif()
if(CMAKE_GENERATOR_TOOLSET)
    list(APPEND consumer_generator_args --build-generator-toolset ${CMAKE_GENERATOR_TOOLSET})
endif()

add_test(NAME InstalledPackageConsumer
    COMMAND ${CMAKE_CTEST_COMMAND}
        --build-and-test ${CMAKE_CURRENT_SOURCE_DIR}/install_consumer ${CMAKE_CURRENT_BINARY_DIR}/install_consumer
        ${consumer_generator_args}
        --build-config $<CONFIG>
        --build-noclean
        --build-options ${consumer_options}
)
set_tests_properties(InstalledPackageConsumer PROPERTIES FIXTURES_REQUIRED slick_socket_installed)
