cmake_minimum_required(VERSION 3.25)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(slick-socket
  VERSION 1.2.0
  LANGUAGES C CXX
)

# Options
option(BUILD_SLICK_SOCKET_EXAMPLES "Build examples" ON)
option(BUILD_SLICK_SOCKET_TESTING "Build tests" ON)
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
# Defaults to ON only when this is the top-level project, so a newer compiler's warnings cannot break
# a consumer that builds these tests/examples through add_subdirectory/FetchContent
option(SLICK_SOCKET_WARNINGS_AS_ERRORS "Treat warnings as errors in tests and examples" ${PROJECT_IS_TOP_LEVEL})

# Strict warnings for this project's own targets. The headers are compiled by consumers at any warning
# level, so the tests/examples build them at a high one. Never applied to the slick::socket interface target.
function(slick_socket_enable_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4 /permissive- $<$<BOOL:${SLICK_SOCKET_WARNINGS_AS_ERRORS}>:/WX>)
  else()
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic $<$<BOOL:${SLICK_SOCKET_WARNINGS_AS_ERRORS}>:-Werror>)
  endif()
endfunction()

if(WIN32)
  # Find wepoll (vcpkg installation without CMake config)
  find_path(WEPOLL_INCLUDE_DIR wepoll.h)
  find_library(WEPOLL_LIBRARY NAMES wepoll)

  if(WEPOLL_INCLUDE_DIR AND WEPOLL_LIBRARY)
    # Create an imported target for wepoll
    add_library(wepoll::wepoll STATIC IMPORTED)
    set_target_properties(wepoll::wepoll PROPERTIES
      IMPORTED_LOCATION "${WEPOLL_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${WEPOLL_INCLUDE_DIR}"
    )
    message(STATUS "wepoll: found at ${WEPOLL_LIBRARY}")
  else()
    # Fallback: Fetch wepoll from GitHub and build it
    message(STATUS "wepoll not found locally, fetching from GitHub...")
    include(FetchContent)
    FetchContent_Declare(
      wepoll
      GIT_REPOSITORY https://github.com/piscisaureus/wepoll.git
      GIT_TAG v1.5.8
    )
    FetchContent_MakeAvailable(wepoll)

    # Build wepoll as a static library
    add_library(wepoll_lib STATIC ${wepoll_SOURCE_DIR}/wepoll.c)
    target_include_directories(wepoll_lib PUBLIC
      $<BUILD_INTERFACE:${wepoll_SOURCE_DIR}>
      $<INSTALL_INTERFACE:include>
    )

    # Set export properties. The debug postfix lets Debug and Release builds install side by side,
    # so one install tree serves consumers of both configurations without mixing C runtimes.
    set_target_properties(wepoll_lib PROPERTIES EXPORT_NAME wepoll DEBUG_POSTFIX d)

    # Create the wepoll::wepoll target
    add_library(wepoll::wepoll ALIAS wepoll_lib)

    message(STATUS "wepoll: fetched and built from GitHub")

    # Mark that we need to install wepoll
    set(SLICK_SOCKET_INSTALL_WEPOLL TRUE)
  endif()
endif()

# MSVC configuration
if(MSVC)
  # Enable policy CMP0091 to allow CMAKE_MSVC_RUNTIME_LIBRARY to be controlled by toolchain/user
  cmake_policy(SET CMP0091 NEW)
  
  # Only set default if not already specified by user or toolchain (e.g., vcpkg)
  if(NOT DEFINED CMAKE_MSVC_RUNTIME_LIBRARY)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
  endif()
endif()


if(ENABLE_ASAN)
  if(MSVC)
    # MSVC AddressSanitizer (requires Visual Studio 2022 17.7+)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "19.37")
      add_compile_options(/fsanitize=address)
      add_link_options(/INCREMENTAL:NO)

      # Additional MSVC ASan flags for better compatibility
      add_compile_definitions(_DISABLE_VECTOR_ANNOTATION)
      add_compile_definitions(_DISABLE_STRING_ANNOTATION)

      message(STATUS "AddressSanitizer enabled for MSVC ${CMAKE_CXX_COMPILER_VERSION}")
    else()
      message(WARNING "AddressSanitizer requires Visual Studio 2022 17.7+ (current: ${CMAKE_CXX_COMPILER_VERSION})")
      set(ENABLE_ASAN OFF CACHE BOOL "ASan disabled - compiler too old" FORCE)
    endif()
  else()
    # GCC/Clang AddressSanitizer
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
  endif()
endif()

if (MSVC)
  set(CMAKE_SUPPRESS_REGENERATION true)   # supress zero_check project
endif()

# Create header-only INTERFACE library
add_library(slick-socket INTERFACE)
add_library(slick::socket ALIAS slick-socket)

target_include_directories(slick-socket INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

# Propagates to every consumer, including installed-package ones; CMAKE_CXX_STANDARD above only covers this project
target_compile_features(slick-socket INTERFACE cxx_std_20)

# Platform-specific dependencies
if(WIN32)
  target_compile_definitions(slick-socket INTERFACE _WIN32_WINNT=0x0601)
  target_link_libraries(slick-socket INTERFACE ws2_32 wepoll::wepoll)
endif()

set_target_properties(slick-socket PROPERTIES EXPORT_NAME socket)

if (BUILD_SLICK_SOCKET_EXAMPLES)
  add_subdirectory(examples)
endif()

# Tests
if(BUILD_SLICK_SOCKET_TESTING)
  enable_testing()
  add_subdirectory(tests)
endif()

# Install headers
install(DIRECTORY include/ DESTINATION include)

# Install wepoll headers if we fetched it
if(SLICK_SOCKET_INSTALL_WEPOLL)
  install(FILES ${wepoll_SOURCE_DIR}/wepoll.h DESTINATION include)
endif()

# Install library targets (header-only INTERFACE library)
install(TARGETS slick-socket
  EXPORT slick-socketTargets
)

# Install wepoll_lib if we built it
if(SLICK_SOCKET_INSTALL_WEPOLL)
  install(TARGETS wepoll_lib
    EXPORT slick-socketTargets
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
  )
endif()

# Install CMake package configuration files
include(CMakePackageConfigHelpers)

# Generate the config file from template
configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/slick-socketConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/slick-socketConfig.cmake
  INSTALL_DESTINATION lib/cmake/slick-socket
)

# Generate version file
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/slick-socketConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)

# Install the config and version files
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/slick-socketConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/slick-socketConfigVersion.cmake
  DESTINATION lib/cmake/slick-socket
)

# Install the targets file
install(EXPORT slick-socketTargets
  FILE slick-socketTargets.cmake
  NAMESPACE slick::
  DESTINATION lib/cmake/slick-socket
)

message(STATUS "${PROJECT_NAME}: ${PROJECT_VERSION}")
