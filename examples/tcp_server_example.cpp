#include "logger.h"
#include <slick/socket/tcp_server.h>
#include <iostream>
#include <string>

class TCPServer : public slick::socket::TCPServerBase<TCPServer>
{
public:
    TCPServer(const slick::socket::TCPServerConfig& config)
        : slick::socket::TCPServerBase<TCPServer>("Echo TCP Server", config)
    {
    }

    void onClientConnected(int client_id, const std::string& client_address)
    {
        LOG_INFO("{} client connected: ID={}, Address={}", name_, client_id, client_address);
    }

    void onClientDisconnected(int client_id)
    {
        LOG_INFO("{} client disconnected: ID={}", name_, client_id);
    }

    void onClientData(int client_id, const uint8_t* data, size_t length)
    {
        std::string msg((char*)data, length);
        std::cout << "Data received from client ID=" << client_id << ", " << msg << std::endl;
        send_data(client_id, msg);
    }
};

int main()
{
    slick::socket::TCPServerConfig config;
    config.port = 9090; // Set custom port if needed
    config.max_connections = 50; // Set max connections
    config.receive_buffer_size = 8192; // Set receive buffer size
    config.idle_timeout = std::chrono::milliseconds(60000); // Disconnect clients idle for 60 s

    TCPServer server(config);

    if (!server.start())
    {
        std::cerr << "Failed to start server." << std::endl;
        return -1;
    }

    std::cout << "Press Enter to stop the server..." << std::endl;
    std::cin.get();

    server.stop();
    return 0;
}