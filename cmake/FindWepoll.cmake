# Shared by the build tree and the installed package. Preserve the legacy
# WEPOLL_LIBRARY override as a configuration-independent library.
find_path(WEPOLL_INCLUDE_DIR wepoll.h)

# Earlier versions cached the auto-found library as WEPOLL_LIBRARY, marked by find_library's default doc
# string. Drop it so it is not taken for an override and the per-configuration search below runs.
get_property(_wepoll_library_doc CACHE WEPOLL_LIBRARY PROPERTY HELPSTRING)
if(_wepoll_library_doc STREQUAL "Path to a library.")
    unset(WEPOLL_LIBRARY CACHE)
endif()

if(WEPOLL_INCLUDE_DIR AND NOT WEPOLL_LIBRARY)
    # The vcpkg toolchain searches debug/lib before lib for Debug and multi-config builds. Its headers are
    # never under debug/, so the lib directory next to them holds the Release library: search it first.
    get_filename_component(_wepoll_root "${WEPOLL_INCLUDE_DIR}" DIRECTORY)
    find_library(WEPOLL_LIBRARY_RELEASE NAMES wepoll HINTS "${_wepoll_root}/lib" NO_DEFAULT_PATH)
    find_library(WEPOLL_LIBRARY_RELEASE NAMES wepoll)
    # vcpkg installs identically named Debug libraries under debug/lib.
    set(_wepoll_debug_hints "${_wepoll_root}/debug/lib")
    if(WEPOLL_LIBRARY_RELEASE)
        get_filename_component(_wepoll_release_dir "${WEPOLL_LIBRARY_RELEASE}" DIRECTORY)
        list(APPEND _wepoll_debug_hints "${_wepoll_release_dir}/../debug/lib")
    endif()
    find_library(WEPOLL_LIBRARY_DEBUG NAMES wepoll wepolld
        HINTS ${_wepoll_debug_hints} NO_DEFAULT_PATH)
    find_library(WEPOLL_LIBRARY_DEBUG NAMES wepolld)
endif()

if(WEPOLL_INCLUDE_DIR AND (WEPOLL_LIBRARY OR WEPOLL_LIBRARY_RELEASE OR WEPOLL_LIBRARY_DEBUG))
    if(NOT TARGET wepoll::wepoll)
        add_library(wepoll::wepoll STATIC IMPORTED)
        set_target_properties(wepoll::wepoll PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${WEPOLL_INCLUDE_DIR}")
        if(WEPOLL_LIBRARY)
            set_target_properties(wepoll::wepoll PROPERTIES IMPORTED_LOCATION "${WEPOLL_LIBRARY}")
        else()
            foreach(_wepoll_config RELEASE DEBUG)
                if(WEPOLL_LIBRARY_${_wepoll_config})
                    set_property(TARGET wepoll::wepoll APPEND PROPERTY IMPORTED_CONFIGURATIONS ${_wepoll_config})
                    set_property(TARGET wepoll::wepoll PROPERTY IMPORTED_LOCATION_${_wepoll_config}
                        "${WEPOLL_LIBRARY_${_wepoll_config}}")
                endif()
            endforeach()
            # Other configurations use Release; a one-library install serves all configurations.
            if(WEPOLL_LIBRARY_RELEASE)
                set(_wepoll_fallback "${WEPOLL_LIBRARY_RELEASE}")
            else()
                set(_wepoll_fallback "${WEPOLL_LIBRARY_DEBUG}")
            endif()
            set_target_properties(wepoll::wepoll PROPERTIES IMPORTED_LOCATION "${_wepoll_fallback}")
        endif()
    endif()
endif()
