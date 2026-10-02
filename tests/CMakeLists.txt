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

# Discover tests with increased timeout
gtest_discover_tests(tests
    DISCOVERY_TIMEOUT 30
)

# Set test properties
set_target_properties(tests PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests
)
