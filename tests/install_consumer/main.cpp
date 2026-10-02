// Builds against an installed slick-socket package, as an external consumer would

// The project sets no C++ standard, so the mode must come from slick::socket's usage requirements.
// Checked explicitly so compilers whose default mode happens to accept the headers still fail.
#if defined(_MSVC_LANG)
static_assert(_MSVC_LANG >= 202002L, "slick::socket must propagate its C++20 requirement");
#else
static_assert(__cplusplus >= 202002L, "slick::socket must propagate its C++20 requirement");
#endif

#include <slick/socket/tcp_server.h>
#include <slick/socket/tcp_client.h>
#include <slick/socket/multicast_sender.h>
#include <slick/socket/multicast_receiver.h>

class Server : public slick::socket::TCPServerBase<Server>
{
public:
    using TCPServerBase::TCPServerBase;
    void onClientConnected(int, const std::string&) {}
    void onClientDisconnected(int) {}
    void onClientData(int client_id, const uint8_t* data, size_t size)
    {
        send_data(client_id, std::vector<uint8_t>(data, data + size));
    }
};

class Client : public slick::socket::TCPClientBase<Client>
{
public:
    using TCPClientBase::TCPClientBase;
    void onConnected() {}
    void onDisconnected() {}
    void onData(const uint8_t*, size_t) {}
};

class Receiver : public slick::socket::MulticastReceiverBase<Receiver>
{
public:
    using MulticastReceiverBase::MulticastReceiverBase;
    void handle_multicast_data(const std::vector<uint8_t>&, const std::string&) {}
};

int main(int argc, char**)
{
    // Instantiates every component without opening sockets when run with no arguments
    if (argc > 1)
    {
        Server server("server");
        Client client("client");
        Receiver receiver("receiver");
        slick::socket::MulticastSender sender("sender");
        server.start();
        client.connect();
        receiver.start();
        sender.start();
        sender.stop();
        receiver.stop();
        client.disconnect();
        server.stop();
    }
    return 0;
}
