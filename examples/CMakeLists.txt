set(SLICK_SOCKET_EXAMPLES
    tcp_server_example
    tcp_client_example
    multicast_sender_example
    multicast_receiver_example
    multicast_integration_example
)

foreach(example IN LISTS SLICK_SOCKET_EXAMPLES)
    add_executable(${example} ${example}.cpp)
    target_include_directories(${example} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(${example} PRIVATE slick::socket)
    slick_socket_enable_warnings(${example})
    set_target_properties(${example} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/examples
    )
endforeach()
