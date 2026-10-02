// Build-type macros must come only from CMake's per-configuration defaults. Deriving them from
// CMAKE_BUILD_TYPE once gave every configuration of a multi-config generator the same DEBUG/NDEBUG,
// so e.g. the Visual Studio Release configuration was compiled with both.
#if defined(DEBUG)
#error "The build must not define DEBUG"
#endif

#if SLICK_SOCKET_TEST_DEBUG_CONFIG && defined(NDEBUG)
#error "NDEBUG is defined in a Debug configuration"
#endif
