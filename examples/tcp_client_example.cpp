#include "logger.h"
#include <slick/socket/tcp_client.h>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

namespace {
    std::atomic_bool data_received_{false};
}

class TCPClient : public slick::socket::TCPClientBase<TCPClient>
{
public:
    TCPClient(const slick::socket::TCPClientConfig& config)
        : slick::socket::TCPClientBase<TCPClient>("Tcp Client", config)
    {
    }

    void onConnected()
    {
        LOG_INFO("Successfully connected to server");
    }

    void onDisconnected()
    {
        LOG_INFO("Successfully disconnected from server");
    }

    void onData(const uint8_t* data, size_t length)
    {
        std::string received_data((const char*)data, length);
        std::cout << "Data received from server: \n" << received_data << std::endl;
        data_received_.store(true, std::memory_order_release);
    }
};

int main()
{
    slick::socket::TCPClientConfig config;
    config.server_address = "127.0.0.1"; // Server address
    config.server_port = 9090; // Server port (should match server example)
    config.receive_buffer_size = 4096;
    config.connection_timeout = std::chrono::milliseconds(5000);

    TCPClient client(config);

    client.connect();

    if (client.is_connected())
    {
        // Send some test data
        std::string test_message = "Hello from TCP client!";
        client.send_data(test_message);

        // Wait for the response
        while(!data_received_.load(std::memory_order_relaxed));

        client.disconnect();
    }
    else
    {
        std::cerr << "Failed to connect to server." << std::endl;
        return -1;
    }

    return 0;
}
