#include <gtest/gtest.h>
#include <slick/socket/multicast_sender.h>
#include <slick/socket/polled_multicast_receiver.h>
#include <slick/socket/polled_tcp_client.h>
#include <slick/socket/tcp_server.h>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

using namespace slick::socket;

// The caller-driven sockets own a socket handle, so two objects must never own the same one
static_assert(!std::is_copy_constructible_v<PolledTCPClient>);
static_assert(!std::is_move_constructible_v<PolledTCPClient>);
static_assert(!std::is_copy_constructible_v<PolledMulticastReceiver>);
static_assert(!std::is_move_constructible_v<PolledMulticastReceiver>);

namespace
{

// Echoes what it receives, and can greet, go quiet or hang up on request
class EchoServer : public TCPServerBase<EchoServer>
{
public:
    using TCPServerBase<EchoServer>::TCPServerBase;

    void onClientConnected(int client_id, const std::string&)
    {
        last_client = client_id;
        ++connected;
        if (!greeting.empty())
        {
            send_data(client_id, greeting);
        }
    }

    void onClientDisconnected(int) { ++disconnected; }

    void onClientData(int client_id, const uint8_t* data, size_t size)
    {
        bytes_received += size;
        if (hang_up_on_data)
        {
            disconnect_client(client_id);
        }
        else if (echo)
        {
            send_data(client_id, data, size);
        }
    }

    std::string greeting;
    std::atomic<bool> echo{true};
    std::atomic<bool> hang_up_on_data{false};
    std::atomic<int> connected{0};
    std::atomic<int> disconnected{0};
    std::atomic<int> last_client{-1};
    std::atomic<size_t> bytes_received{0};
};

// Runs `step` - one turn of the caller's loop - until `done` or the deadline. Returns whether it was done.
bool drive(const std::function<bool()>& done, const std::function<void()>& step = [] {},
           std::chrono::milliseconds timeout = std::chrono::milliseconds(3000))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!done())
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return false;
        }
        step();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

class PolledTCPClientTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        TCPServerConfig server_config;
        server_config.port = 0;  // OS-assigned, so tests never collide on a port
        server_ = std::make_unique<EchoServer>("EchoServer", server_config);
        ASSERT_TRUE(server_->start());

        config_.server_address = "127.0.0.1";
        config_.server_port = server_->port();
        config_.connection_timeout = std::chrono::milliseconds(2000);
    }

    void TearDown() override { server_->stop(); }

    // connect(), then poll() until the connection is made
    bool connect(PolledTCPClient& client)
    {
        return client.connect() && drive([&] { return client.poll() != PolledTCPClient::State::Connecting; }) &&
               client.is_connected();
    }

    // Everything the client receives until `size` bytes have arrived or the connection ends
    std::string receive(PolledTCPClient& client, size_t size)
    {
        std::string out;
        std::array<uint8_t, 4096> buffer{};
        drive([&] { return out.size() >= size || !client.is_connected(); }, [&] {
            const std::ptrdiff_t received = client.receive(buffer);
            if (received > 0)
            {
                out.append(reinterpret_cast<const char*>(buffer.data()), static_cast<size_t>(received));
            }
        });
        return out;
    }

    static std::span<const uint8_t> bytes(const std::string& text)
    {
        return {reinterpret_cast<const uint8_t*>(text.data()), text.size()};
    }

    std::unique_ptr<EchoServer> server_;
    PolledTCPClientConfig config_;
};

} // namespace

TEST_F(PolledTCPClientTest, ConnectReturnsAtOnceAndPollCompletesIt)
{
    PolledTCPClient client("Client", config_);
    EXPECT_EQ(client.state(), PolledTCPClient::State::Disconnected);

    ASSERT_TRUE(client.connect());
    EXPECT_NE(client.state(), PolledTCPClient::State::Disconnected);
    ASSERT_TRUE(drive([&] { return client.poll() == PolledTCPClient::State::Connected; }));
    EXPECT_TRUE(client.is_connected());
    EXPECT_TRUE(drive([&] { return server_->connected.load() == 1; }));
}

TEST_F(PolledTCPClientTest, SendsAndReceivesWithoutAThreadOfItsOwn)
{
    PolledTCPClient client("Client", config_);
    ASSERT_TRUE(connect(client));

    // Nothing has arrived yet: receive() says so instead of waiting
    std::array<uint8_t, 64> buffer{};
    EXPECT_EQ(client.receive(buffer), 0);

    const std::string message = "polled hello";
    ASSERT_TRUE(client.send(bytes(message)));
    EXPECT_EQ(receive(client, message.size()), message);
}

TEST_F(PolledTCPClientTest, ReceivesWhatTheServerSentBeforeTheFirstPoll)
{
    server_->greeting = "hello";
    PolledTCPClient client("Client", config_);
    ASSERT_TRUE(connect(client));
    EXPECT_EQ(receive(client, 5), "hello");
}

TEST_F(PolledTCPClientTest, LargeSendArrivesWhole)
{
    PolledTCPClient client("Client", config_);
    ASSERT_TRUE(connect(client));
    server_->echo = false;

    // Far more than the socket buffer holds: send() waits for the server to drain it
    std::string big(4 * 1024 * 1024, 'x');
    ASSERT_TRUE(client.send(bytes(big)));
    EXPECT_TRUE(drive([&] { return server_->bytes_received.load() == big.size(); }));
}

TEST_F(PolledTCPClientTest, ServerCloseIsReportedByReceive)
{
    PolledTCPClient client("Client", config_);
    ASSERT_TRUE(connect(client));
    server_->hang_up_on_data = true;
    ASSERT_TRUE(client.send(bytes("bye")));

    std::array<uint8_t, 64> buffer{};
    std::ptrdiff_t result = 0;
    ASSERT_TRUE(drive([&] { return result < 0; }, [&] { result = client.receive(buffer); }));
    EXPECT_EQ(client.state(), PolledTCPClient::State::Disconnected);
    EXPECT_FALSE(client.send(bytes("late")));
    EXPECT_EQ(client.receive(buffer), -1);
}

TEST_F(PolledTCPClientTest, ReconnectsAfterADisconnect)
{
    PolledTCPClient client("Client", config_);
    ASSERT_TRUE(connect(client));
    client.disconnect();
    EXPECT_EQ(client.state(), PolledTCPClient::State::Disconnected);
    EXPECT_TRUE(drive([&] { return server_->disconnected.load() == 1; }));

    ASSERT_TRUE(connect(client));
    ASSERT_TRUE(client.send(bytes("again")));
    EXPECT_EQ(receive(client, 5), "again");
}

TEST_F(PolledTCPClientTest, RefusedConnectionEndsDisconnected)
{
    server_->stop();  // nothing listens on the port any more
    PolledTCPClient client("Client", config_);
    if (client.connect())
    {
        // Refused, or - where the stack drops the attempt silently - timed out; never left pending
        EXPECT_TRUE(drive([&] { return client.poll() == PolledTCPClient::State::Disconnected; }, [] {},
                          config_.connection_timeout + std::chrono::milliseconds(2000)));
    }
    EXPECT_FALSE(client.is_connected());
    std::array<uint8_t, 8> buffer{};
    EXPECT_EQ(client.receive(buffer), -1);
}

TEST_F(PolledTCPClientTest, UnresolvableAddressFailsToStart)
{
    config_.server_address = "no-such-host.invalid";
    PolledTCPClient client("Client", config_);
    EXPECT_FALSE(client.connect());
    EXPECT_EQ(client.state(), PolledTCPClient::State::Disconnected);
}

// ---------------------------------------------------------------------------
// PolledMulticastReceiver
// ---------------------------------------------------------------------------
namespace
{

class PolledMulticastReceiverTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        receiver_config_.multicast_address = "224.0.0.103";
        receiver_config_.port = 12371;
        receiver_config_.receive_buffer_size = 1 << 20;

        sender_config_.multicast_address = receiver_config_.multicast_address;
        sender_config_.port = receiver_config_.port;
        sender_config_.enable_loopback = true;  // the receiver is on the sending host
    }

    // Polls until a datagram arrives, and returns it
    std::string receive_one(PolledMulticastReceiver& receiver, std::span<uint8_t> buffer)
    {
        std::ptrdiff_t received = 0;
        drive([&] { return received != 0; }, [&] { received = receiver.receive(buffer); });
        return received > 0 ? std::string(reinterpret_cast<const char*>(buffer.data()), static_cast<size_t>(received))
                            : std::string();
    }

    PolledMulticastReceiverConfig receiver_config_;
    MulticastSenderConfig sender_config_;
};

// Sends `message`. Returns an empty string when that worked, otherwise why not: the OS error sendto()
// left, which send_data() does not overwrite on its way out. The error is cleared first, so a send that
// failed before reaching sendto() is not blamed on an earlier call.
std::string send_failure(MulticastSender& sender, const std::string& message)
{
#if defined(_WIN32) || defined(_WIN64)
    WSASetLastError(0);
#else
    errno = 0;
#endif
    if (sender.send_data(message))
    {
        return {};
    }

    const int error = detail::last_socket_error();
    if (error == 0)
    {
        return "send_data() failed without an OS error";
    }
#if defined(_WIN32) || defined(_WIN64)
    return "error " + std::to_string(error);
#else
    return "error " + std::to_string(error) + " (" + std::strerror(error) + ")";
#endif
}

} // namespace

// A test's first send. Where this host cannot send multicast at all, as on GitHub's macOS runners, there
// is nothing for the receiver to read, so the test is skipped as the MulticastSender tests are, saying
// why the send failed. Once one send has worked, later ones must too.
#define SEND_FIRST_OR_SKIP(sender, message)                                                      \
    if (const std::string failure = send_failure((sender), (message)); !failure.empty())        \
    {                                                                                            \
        GTEST_SKIP() << "Multicast sending not available in this environment: " << failure;     \
    }

TEST_F(PolledMulticastReceiverTest, OpensAndCloses)
{
    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    EXPECT_FALSE(receiver.is_open());
    ASSERT_TRUE(receiver.open());
    EXPECT_TRUE(receiver.is_open());
    EXPECT_TRUE(receiver.open());  // already open

    // Nothing has been sent: receive() says so instead of waiting
    std::array<uint8_t, 64> buffer{};
    EXPECT_EQ(receiver.receive(buffer), 0);

    receiver.close();
    EXPECT_FALSE(receiver.is_open());
    receiver.close();  // closing twice is harmless
}

TEST_F(PolledMulticastReceiverTest, InvalidGroupFailsToOpen)
{
    receiver_config_.multicast_address = "not-an-address";
    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    EXPECT_FALSE(receiver.open());
    EXPECT_FALSE(receiver.is_open());
}

TEST_F(PolledMulticastReceiverTest, ReceivesDatagramsOneAtATimeInOrder)
{
    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    ASSERT_TRUE(receiver.open());
    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());

    SEND_FIRST_OR_SKIP(sender, "first");
    ASSERT_TRUE(sender.send_data(std::string("second datagram")));

    // Each call hands over exactly one datagram: boundaries are kept
    std::array<uint8_t, 1500> buffer{};
    EXPECT_EQ(receive_one(receiver, buffer), "first");
    EXPECT_EQ(receive_one(receiver, buffer), "second datagram");
    EXPECT_EQ(receiver.receive(buffer), 0);

    EXPECT_EQ(receiver.get_packets_received(), 2u);
    EXPECT_EQ(receiver.get_bytes_received(), 5u + 15u);
    EXPECT_EQ(receiver.get_receive_errors(), 0u);
    EXPECT_EQ(receiver.get_packets_truncated(), 0u);
    sender.stop();
}

TEST_F(PolledMulticastReceiverTest, DatagramLongerThanTheBufferIsCutAndCounted)
{
    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    ASSERT_TRUE(receiver.open());
    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());

    SEND_FIRST_OR_SKIP(sender, "0123456789");
    ASSERT_TRUE(sender.send_data(std::string("next")));

    std::array<uint8_t, 4> tiny{};
    EXPECT_EQ(receive_one(receiver, tiny), "0123");
    EXPECT_EQ(receiver.get_packets_truncated(), 1u);

    // The rest of the long datagram is gone, not prepended to the next one
    std::array<uint8_t, 64> buffer{};
    EXPECT_EQ(receive_one(receiver, buffer), "next");
    EXPECT_EQ(receiver.get_packets_received(), 2u);
    sender.stop();
}

TEST_F(PolledMulticastReceiverTest, TwoReceiversOnOnePortBothHearTheGroup)
{
    PolledMulticastReceiver first("First", receiver_config_);
    PolledMulticastReceiver second("Second", receiver_config_);
    ASSERT_TRUE(first.open());
    ASSERT_TRUE(second.open());
    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, "both");

    std::array<uint8_t, 64> buffer{};
    EXPECT_EQ(receive_one(first, buffer), "both");
    EXPECT_EQ(receive_one(second, buffer), "both");
    sender.stop();
}

TEST_F(PolledMulticastReceiverTest, ReopensAfterClose)
{
    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    ASSERT_TRUE(receiver.open());
    receiver.close();
    ASSERT_TRUE(receiver.open());

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, "again");
    std::array<uint8_t, 64> buffer{};
    EXPECT_EQ(receive_one(receiver, buffer), "again");
    sender.stop();
}
