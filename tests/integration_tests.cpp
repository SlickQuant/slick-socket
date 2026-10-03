#include <gtest/gtest.h>
#include <atomic>

// Count contract violations instead of asserting, so tests can check for them
static std::atomic<int> g_unsafe_destroys{0};
#define SLICK_SOCKET_ON_UNSAFE_DESTROY() (++g_unsafe_destroys)

#include "../examples/logger.h"
#include <slick/socket/tcp_server.h>
#include <slick/socket/tcp_client.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <algorithm>
#include <cstring>
#include <optional>

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#else
#include <pthread.h>
#include <signal.h>
#include <time.h>
#endif

// Called from a callback after the peer closed the connection but before the event loop has seen it.
// The first write draws a reset from the peer, so a later one fails with EPIPE/ECONNRESET, which must
// be reported as a failed send rather than raised as SIGPIPE. Returns whether a send failed.
template<typename SendFn>
static bool write_until_send_fails(SendFn send)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // let the peer's close arrive
    for (int i = 0; i < 20; ++i) {
        if (!send()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

class IntegrationTestServer : public slick::socket::TCPServerBase<IntegrationTestServer>
{
public:
    using slick::socket::TCPServerBase<IntegrationTestServer>::TCPServerBase;
    using slick::socket::TCPServerBase<IntegrationTestServer>::get_connected_client_count;
    using slick::socket::TCPServerBase<IntegrationTestServer>::send_data;

    void onClientConnected(int client_id, const std::string&) {
        connected_clients++;
        last_connected_client_id = client_id;
        if (greet_on_connect) {
            send_data(client_id, std::string("hello"));
        }
        if (kick_on_connect) {
            kick(client_id);
        }
    }

    void onClientDisconnected(int client_id) {
        disconnected_clients++;
        last_disconnected_client_id = client_id;
    }

    // Disconnects the client from within a callback; the notification must not re-enter this callback
    void kick(int client_id) {
        disconnect_client(client_id);
        disconnect_client(client_id);  // already gone: must not notify twice
        notified_during_kick = disconnected_clients.load() != 0;
    }

    void onClientData(int client_id, const uint8_t* data, size_t length) {
        if (length == 4 && std::memcmp(data, "kick", 4) == 0) {
            kick(client_id);
            return;
        }
        if (length == 9 && std::memcmp(data, "send-late", 9) == 0) {
            // The client closes right after sending this
            late_send_failed = write_until_send_fails([&]() { return send_data(client_id, std::string("late")); });
            return;
        }
        if (length == 3 && std::memcmp(data, "bye", 3) == 0) {
            send_data(client_id, std::string("bye"));
            disconnect_client(client_id);
            return;
        }
        if (length == 4 && std::memcmp(data, "push", 4) == 0) {
            send_data(push_target.load(), std::string("tick"));  // server-initiated traffic to another client
            return;
        }
        if (stop_on_data) {
            stop();  // stopping from within a callback must not self-join the server thread
            return;
        }
        while (hold) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));  // simulate a stalled server
        }
        if (length == 5 && std::memcmp(data, "flood", 5) == 0) {
            flood(client_id);
            return;
        }
        data_received++;
        bytes_received += length;
        last_data_client_id = client_id;
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            last_received_data_.assign((const char*)data, length);
        }

        if (!echo) {
            return;
        }

        // Echo the data back to the client
        std::vector<uint8_t> buffer(data, data + length);
        send_data(client_id, buffer);
    }

    std::string last_received_data() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        return last_received_data_;
    }

    std::thread& server_thread() { return server_thread_; }

    // Sends flood_payload to the client in flood_chunk-sized messages from within a single callback
    void flood(int client_id) {
        const size_t chunk = flood_chunk;
        for (size_t offset = 0; offset < flood_payload.size(); offset += chunk) {
            const size_t size = (std::min)(chunk, flood_payload.size() - offset);
            std::vector<uint8_t> message(flood_payload.begin() + offset, flood_payload.begin() + offset + size);
            if (send_data(client_id, message)) {
                flood_accepted += size;
            } else {
                send_failures++;
            }
        }
        flood_done = true;
    }

    std::vector<uint8_t> flood_payload;  // set before the flood is triggered
    size_t flood_chunk = 1024 * 1024;
    std::atomic<size_t> flood_accepted{0};
    std::atomic<int> send_failures{0};
    std::atomic<bool> flood_done{false};
    std::atomic<bool> hold{false};
    std::atomic<bool> echo{true};

    std::atomic<int> connected_clients{0};
    std::atomic<int> disconnected_clients{0};
    std::atomic<int> data_received{0};
    std::atomic<size_t> bytes_received{0};
    std::atomic<bool> stop_on_data{false};
    std::atomic<bool> kick_on_connect{false};
    std::atomic<bool> greet_on_connect{false};
    std::atomic<bool> notified_during_kick{false};
    std::atomic<int> push_target{-1};
    std::atomic<bool> late_send_failed{false};
    std::atomic<int> last_connected_client_id{-1};
    std::atomic<int> last_disconnected_client_id{-1};
    std::atomic<int> last_data_client_id{-1};

private:
    std::mutex data_mutex_;
    std::string last_received_data_;
};

class IntegrationTestClient : public slick::socket::TCPClientBase<IntegrationTestClient>
{
public:
    using slick::socket::TCPClientBase<IntegrationTestClient>::TCPClientBase;

    void onConnected() {
        if (slow_on_connected) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));  // post-connect setup
        }
        if (disconnect_in_on_connected.exchange(false)) {
            disconnect();
        }
        if (reconnect_in_on_connected.exchange(false)) {
            disconnect();
            connect();
        }
        connected_count++;
        connection_established = true;
        on_connected_returned = true;
    }

    void onDisconnected() {
        if (connect_in_on_disconnected) {
            callback_connect_result = connect() ? 1 : 0;  // runs on the client thread
        }
        disconnected_count++;
        connection_established = false;
        if (external_disconnects) {
            (*external_disconnects)++;
        }
    }

    void onData(const uint8_t* data, size_t length) {
        if (!on_connected_returned) {
            data_before_on_connected = true;
        }
        if (length == 3 && std::memcmp(data, "bye", 3) == 0) {
            // The server closed the connection right after sending this
            late_send_failed = write_until_send_fails([this]() { return send_data(std::string("late")); });
            return;
        }
        while (hold) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));  // simulate a slow reader
        }
        data_received_count++;
        bytes_received += length;
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            received_data_.append((const char*)data, length);
        }
        data_received_flag = true;
    }

    std::string received_data() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        return received_data_;
    }

    std::thread& client_thread() { return client_thread_; }

    std::atomic<int> connected_count{0};
    std::atomic<int> disconnected_count{0};
    std::atomic<int> data_received_count{0};
    std::atomic<size_t> bytes_received{0};
    std::atomic<bool> connection_established{false};
    std::atomic<bool> data_received_flag{false};
    std::atomic<int>* external_disconnects = nullptr;  // outlives the client, unlike the members above
    std::atomic<bool> hold{false};
    std::atomic<bool> late_send_failed{false};
    std::atomic<bool> connect_in_on_disconnected{false};
    std::atomic<int> callback_connect_result{-1};
    std::atomic<bool> slow_on_connected{false};
    std::atomic<bool> on_connected_returned{false};
    std::atomic<bool> data_before_on_connected{false};
    std::atomic<bool> disconnect_in_on_connected{false};
    std::atomic<bool> reconnect_in_on_connected{false};

private:
    std::mutex data_mutex_;
    std::string received_data_;
};

// CPU time consumed by a thread so far; not measured on macOS (returns -1, leaving `thread` unused)
static std::chrono::nanoseconds thread_cpu_time([[maybe_unused]] std::thread& thread)
{
#if defined(_WIN32) || defined(_WIN64)
    FILETIME creation, exit, kernel, user;
    if (!GetThreadTimes(thread.native_handle(), &creation, &exit, &kernel, &user)) {
        return std::chrono::nanoseconds{-1};
    }
    auto ticks = [](const FILETIME& ft) {
        return (static_cast<uint64_t>(ft.dwHighDateTime) << 32) | ft.dwLowDateTime;
    };
    return std::chrono::nanoseconds{(ticks(kernel) + ticks(user)) * 100};
#elif defined(__APPLE__)
    return std::chrono::nanoseconds{-1};
#else
    clockid_t clock_id;
    timespec ts{};
    if (pthread_getcpuclockid(thread.native_handle(), &clock_id) != 0 || clock_gettime(clock_id, &ts) != 0) {
        return std::chrono::nanoseconds{-1};
    }
    return std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec};
#endif
}

static std::vector<uint8_t> make_payload(size_t size)
{
    std::vector<uint8_t> payload(size);
    for (size_t i = 0; i < size; ++i) {
        payload[i] = static_cast<uint8_t>((i * 31) % 251);
    }
    return payload;
}

// Asserts that a thread used less than a quarter of a 500ms window on the CPU
static void expect_mostly_idle(std::thread& thread, const char* what)
{
    constexpr auto window = std::chrono::milliseconds(500);
    auto cpu_before = thread_cpu_time(thread);
    ASSERT_GE(cpu_before.count(), 0);
    std::this_thread::sleep_for(window);
    auto cpu_used = thread_cpu_time(thread) - cpu_before;

    EXPECT_LT(cpu_used, window / 4)
        << what << " used " << std::chrono::duration_cast<std::chrono::milliseconds>(cpu_used).count()
        << "ms of CPU in " << window.count() << "ms";
}

#if !defined(_WIN32) && !defined(_WIN64)
// Puts SIGPIPE at its default disposition (terminate the process) for a test's duration, so a write
// that raises it ends the test run instead of being masked, and restores the previous disposition after
class DefaultSigpipe
{
public:
    DefaultSigpipe() {
        struct sigaction action{};
        action.sa_handler = SIG_DFL;
        sigemptyset(&action.sa_mask);
        sigaction(SIGPIPE, &action, &saved_);
    }
    ~DefaultSigpipe() { sigaction(SIGPIPE, &saved_, nullptr); }

    DefaultSigpipe(const DefaultSigpipe&) = delete;
    DefaultSigpipe& operator=(const DefaultSigpipe&) = delete;

    static bool is_default() {
        struct sigaction current{};
        sigaction(SIGPIPE, nullptr, &current);
        return current.sa_handler == SIG_DFL;
    }

private:
    struct sigaction saved_{};
};
#endif

class TCPIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use port 0 to let the system assign an available port
        server_config_.port = 0;
        server_config_.max_connections = 10;
        server_config_.receive_buffer_size = 4096;

        client_config_.server_address = "127.0.0.1";
        client_config_.server_port = 0; // Set from server_->port() after the server starts
        client_config_.receive_buffer_size = 4096;
        client_config_.connection_timeout = std::chrono::milliseconds(2000);
    }

    void TearDown() override {
        if (client_) {
            client_->disconnect();
        }
        if (server_ && server_->is_running()) {
            server_->stop();
        }

        // Every object destroyed during the test must have been stopped/disconnected first
        EXPECT_EQ(g_unsafe_destroys.exchange(0), expected_unsafe_destroys_);
    }

    // Helper to wait for condition with timeout
    bool waitForCondition(std::function<bool()> condition, int timeout_ms = 5000) {
        auto start = std::chrono::steady_clock::now();
        while (!condition()) {
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() > timeout_ms) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return true;
    }

    void startServer() {
        server_ = std::make_unique<IntegrationTestServer>("IntegrationServer", server_config_);
        ASSERT_TRUE(server_->start());
        ASSERT_NE(server_->port(), 0);
        client_config_.server_port = server_->port();
    }

    std::unique_ptr<IntegrationTestClient> connectClient(const std::string& name) {
        auto client = std::make_unique<IntegrationTestClient>(name, client_config_);
        EXPECT_TRUE(client->connect());
        return client;
    }

    slick::socket::TCPServerConfig server_config_;
    slick::socket::TCPClientConfig client_config_;
    std::unique_ptr<IntegrationTestServer> server_;
    std::unique_ptr<IntegrationTestClient> client_;
    int expected_unsafe_destroys_ = 0;
};

TEST_F(TCPIntegrationTest, ServerClientLifecycle) {
    ASSERT_NO_FATAL_FAILURE(startServer());

    client_ = std::make_unique<IntegrationTestClient>("IntegrationClient", client_config_);

    // Verify initial states
    EXPECT_EQ(server_->get_connected_client_count(), 0u);
    EXPECT_FALSE(client_->is_connected());

    // Stop server
    server_->stop();
    EXPECT_FALSE(server_->is_running());
}

TEST_F(TCPIntegrationTest, ServerReportsAssignedPort) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    EXPECT_NE(server_->port(), 0);
}

TEST_F(TCPIntegrationTest, EchoRoundTrip) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(client_->is_connected());
    EXPECT_EQ(client_->connected_count.load(), 1);

    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    ASSERT_TRUE(client_->send_data(std::string("hello slick")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->received_data() == "hello slick"; }));
    EXPECT_EQ(server_->last_received_data(), "hello slick");
    EXPECT_EQ(server_->last_data_client_id.load(), server_->last_connected_client_id.load());

    client_->disconnect();
    EXPECT_FALSE(client_->is_connected());
    EXPECT_EQ(client_->disconnected_count.load(), 1);
    ASSERT_TRUE(waitForCondition([this]() { return server_->disconnected_clients.load() == 1; }));
}

// Default client config uses "localhost", which must resolve rather than be rejected by inet_pton
TEST_F(TCPIntegrationTest, DefaultLocalhostAddressConnects) {
    ASSERT_NO_FATAL_FAILURE(startServer());

    slick::socket::TCPClientConfig config;
    config.server_port = server_->port();
    config.connection_timeout = std::chrono::milliseconds(2000);
    ASSERT_EQ(config.server_address, "localhost");

    client_ = std::make_unique<IntegrationTestClient>("LocalhostClient", config);
    ASSERT_TRUE(client_->connect());
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));
}

TEST_F(TCPIntegrationTest, ConnectFailsWhenNoServerListening) {
    // Grab a free port, then release it so nothing listens there
    ASSERT_NO_FATAL_FAILURE(startServer());
    server_->stop();

    // Windows retries a refused SYN for ~2s before failing, so allow more than that
    client_config_.connection_timeout = std::chrono::milliseconds(5000);
    client_ = std::make_unique<IntegrationTestClient>("IntegrationClient", client_config_);
    auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(client_->connect());
    EXPECT_FALSE(client_->is_connected());
    EXPECT_EQ(client_->connected_count.load(), 0);
    EXPECT_LT(std::chrono::steady_clock::now() - start, client_config_.connection_timeout);
}

// A server-initiated close ends the client thread on its own; destroying or reconnecting
// the client afterwards must join that thread instead of terminating the process.
TEST_F(TCPIntegrationTest, ClientSurvivesServerInitiatedDisconnect) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    server_->stop();
    ASSERT_TRUE(waitForCondition([this]() { return client_->disconnected_count.load() == 1; }));
    EXPECT_FALSE(client_->is_connected());

    // Reconnect to a new server on the same port reuses the client object and its thread member
    server_config_.port = server_->port();
    ASSERT_NO_FATAL_FAILURE(startServer());
    ASSERT_TRUE(client_->connect());
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));
    ASSERT_TRUE(client_->send_data(std::string("again")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->received_data() == "again"; }));

    server_->stop();
    ASSERT_TRUE(waitForCondition([this]() { return client_->disconnected_count.load() == 2; }));

    // Destroying the client with a finished-but-unjoined thread must not call std::terminate.
    // Skipping disconnect() breaks the lifetime contract, so it is also reported.
    client_.reset();
    expected_unsafe_destroys_ = 1;
}

// stop() must not tear down connection state while the server thread is still using it
TEST_F(TCPIntegrationTest, StopWithActiveClients) {
    ASSERT_NO_FATAL_FAILURE(startServer());

    std::vector<std::unique_ptr<IntegrationTestClient>> clients;
    for (int i = 0; i < 4; ++i) {
        clients.push_back(connectClient("Client" + std::to_string(i)));
    }
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 4; }));

    std::atomic<bool> sending{true};
    std::vector<std::thread> senders;
    for (auto& client : clients) {
        senders.emplace_back([&sending, c = client.get()]() {
            while (sending.load() && c->is_connected()) {
                c->send_data(std::string("payload"));
            }
        });
    }

    ASSERT_TRUE(waitForCondition([this]() { return server_->data_received.load() > 100; }));
    server_->stop();
    EXPECT_FALSE(server_->is_running());

    sending = false;
    for (auto& t : senders) {
        t.join();
    }
    for (auto& client : clients) {
        EXPECT_TRUE(waitForCondition([&client]() { return !client->is_connected(); }));
        // Join the client thread before the derived object's members are destroyed
        client->disconnect();
    }
}

// Payload larger than receive_buffer_size must be fully delivered without further traffic
TEST_F(TCPIntegrationTest, PayloadLargerThanReceiveBuffer) {
    server_config_.receive_buffer_size = 1024;
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    constexpr size_t payload_size = 64 * 1024;
    std::vector<uint8_t> payload(payload_size);
    for (size_t i = 0; i < payload_size; ++i) {
        payload[i] = static_cast<uint8_t>(i);
    }
    ASSERT_TRUE(client_->send_data(payload));

    ASSERT_TRUE(waitForCondition([this]() { return server_->bytes_received.load() == payload_size; }))
        << "server received " << server_->bytes_received.load() << " of " << payload_size << " bytes";
    ASSERT_TRUE(waitForCondition([this]() { return client_->bytes_received.load() == payload_size; }));
    EXPECT_EQ(client_->received_data(), std::string(payload.begin(), payload.end()));
}

// An idle connected client must not keep the server thread busy (e.g. via EPOLLOUT readiness)
TEST_F(TCPIntegrationTest, IdleClientDoesNotSpinServerThread) {
#if defined(__APPLE__)
    GTEST_SKIP() << "Per-thread CPU time is not measured on macOS";
#endif
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    constexpr auto window = std::chrono::milliseconds(500);
    auto cpu_before = thread_cpu_time(server_->server_thread());
    ASSERT_GE(cpu_before.count(), 0);
    std::this_thread::sleep_for(window);
    auto cpu_used = thread_cpu_time(server_->server_thread()) - cpu_before;

    EXPECT_LT(cpu_used, window / 4)
        << "server thread used " << std::chrono::duration_cast<std::chrono::milliseconds>(cpu_used).count()
        << "ms of CPU in " << window.count() << "ms with an idle client";
}

TEST_F(TCPIntegrationTest, MaxConnectionsEnforced) {
    server_config_.max_connections = 1;
    ASSERT_NO_FATAL_FAILURE(startServer());

    client_ = connectClient("Client1");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    // The TCP handshake completes in the kernel, then the server closes the excess connection
    auto rejected = connectClient("Client2");
    ASSERT_TRUE(waitForCondition([&rejected]() { return rejected->disconnected_count.load() == 1; }));
    EXPECT_FALSE(rejected->is_connected());
    EXPECT_EQ(server_->connected_clients.load(), 1);

    // The first client is unaffected
    ASSERT_TRUE(client_->send_data(std::string("still here")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->received_data() == "still here"; }));

    // A slot frees up once the first client leaves
    client_->disconnect();
    ASSERT_TRUE(waitForCondition([this]() { return server_->disconnected_clients.load() == 1; }));
    ASSERT_TRUE(rejected->connect());
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 2; }));
    ASSERT_TRUE(rejected->send_data(std::string("admitted")));
    ASSERT_TRUE(waitForCondition([&rejected]() { return rejected->received_data() == "admitted"; }));

    // Disconnect before destruction so onDisconnected() never runs on a partially destroyed client
    rejected->disconnect();
}

TEST_F(TCPIntegrationTest, StopFromServerCallback) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    server_->stop_on_data = true;
    ASSERT_TRUE(client_->send_data(std::string("stop")));
    ASSERT_TRUE(waitForCondition([this]() { return !server_->is_running(); }));

    // The server thread releases its sockets on exit, so the client sees the close
    ASSERT_TRUE(waitForCondition([this]() { return client_->disconnected_count.load() == 1; }));
    EXPECT_EQ(server_->get_connected_client_count(), 0u);

    // Restarting joins the finished thread and listens on the same port again
    server_->stop_on_data = false;
    ASSERT_TRUE(server_->start());
    ASSERT_TRUE(client_->connect());
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 2; }));
    ASSERT_TRUE(client_->send_data(std::string("restarted")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->received_data() == "restarted"; }));
}

// disconnect_client() must report onClientDisconnected() exactly once, after the calling callback returns
TEST_F(TCPIntegrationTest, ServerInitiatedDisconnectNotifies) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));
    const int client_id = server_->last_connected_client_id.load();

    ASSERT_TRUE(client_->send_data(std::string("kick")));
    ASSERT_TRUE(waitForCondition([this]() { return server_->disconnected_clients.load() == 1; }));
    EXPECT_EQ(server_->last_disconnected_client_id.load(), client_id);
    EXPECT_FALSE(server_->notified_during_kick.load());
    EXPECT_EQ(server_->get_connected_client_count(), 0u);
    ASSERT_TRUE(waitForCondition([this]() { return client_->disconnected_count.load() == 1; }));

    // No duplicate notification arrives later
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(server_->disconnected_clients.load(), 1);
}

// A client disconnected from onClientConnected() is reported too
TEST_F(TCPIntegrationTest, DisconnectFromConnectCallbackNotifies) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    server_->kick_on_connect = true;
    client_ = connectClient("IntegrationClient");

    ASSERT_TRUE(waitForCondition([this]() { return server_->disconnected_clients.load() == 1; }));
    EXPECT_EQ(server_->last_disconnected_client_id.load(), server_->last_connected_client_id.load());
    EXPECT_FALSE(server_->notified_during_kick.load());
    ASSERT_TRUE(waitForCondition([this]() { return client_->disconnected_count.load() == 1; }));
}

// A client with no traffic for idle_timeout is disconnected and reported
TEST_F(TCPIntegrationTest, IdleClientIsDisconnected) {
    server_config_.idle_timeout = std::chrono::milliseconds(200);
    ASSERT_NO_FATAL_FAILURE(startServer());

    const auto start = std::chrono::steady_clock::now();
    client_ = connectClient("IdleClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->disconnected_clients.load() == 1; }));
    EXPECT_GE(std::chrono::steady_clock::now() - start, server_config_.idle_timeout);
    EXPECT_EQ(server_->last_disconnected_client_id.load(), server_->last_connected_client_id.load());
    EXPECT_EQ(server_->get_connected_client_count(), 0u);
    ASSERT_TRUE(waitForCondition([this]() { return client_->disconnected_count.load() == 1; }));
}

// Received data resets the idle timer
TEST_F(TCPIntegrationTest, ReceivingKeepsClientAlive) {
    server_config_.idle_timeout = std::chrono::milliseconds(300);
    ASSERT_NO_FATAL_FAILURE(startServer());
    server_->echo = false;  // only inbound traffic counts here
    client_ = connectClient("ActiveClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    for (int i = 0; i < 20; ++i) {  // ~1s, over 3x the timeout
        ASSERT_TRUE(client_->send_data(std::string("beat")));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(server_->disconnected_clients.load(), 0);
    EXPECT_TRUE(client_->is_connected());

    // Reaped once it goes quiet
    ASSERT_TRUE(waitForCondition([this]() { return server_->disconnected_clients.load() == 1; }));
}

// A receive-only client stays connected while the server keeps sending to it
TEST_F(TCPIntegrationTest, SendingKeepsClientAlive) {
    server_config_.idle_timeout = std::chrono::milliseconds(300);
    ASSERT_NO_FATAL_FAILURE(startServer());
    server_->echo = false;

    auto listener = connectClient("Listener");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));
    server_->push_target = server_->last_connected_client_id.load();
    client_ = connectClient("Driver");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 2; }));

    // The driver's "push" makes the server send to the listener, which itself never sends
    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(client_->send_data(std::string("push")));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(server_->disconnected_clients.load(), 0);
    EXPECT_TRUE(listener->is_connected());
    EXPECT_GT(listener->bytes_received.load(), 0u);

    listener->disconnect();
}

// With the default idle_timeout (off), a silent client is never reaped
TEST_F(TCPIntegrationTest, IdleTimeoutOffByDefault) {
    EXPECT_EQ(slick::socket::TCPServerConfig{}.idle_timeout.count(), 0);
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IdleClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(server_->disconnected_clients.load(), 0);
    EXPECT_TRUE(client_->is_connected());
}

// Runs each test, including teardown, with SIGPIPE at its default disposition. POSIX only.
class TCPSigpipeTest : public TCPIntegrationTest {
protected:
    void SetUp() override {
#if defined(_WIN32) || defined(_WIN64)
        GTEST_SKIP() << "SIGPIPE is POSIX-only";
#else
        default_sigpipe_.emplace();
        TCPIntegrationTest::SetUp();
#endif
    }

#if !defined(_WIN32) && !defined(_WIN64)
    void TearDown() override {
        TCPIntegrationTest::TearDown();
        default_sigpipe_.reset();
    }

    std::optional<DefaultSigpipe> default_sigpipe_;
#endif
};

// Creating, starting and connecting TCP objects must leave the process's SIGPIPE disposition alone
TEST_F(TCPSigpipeTest, TcpObjectsLeaveSigpipeDispositionAlone) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));
#if !defined(_WIN32) && !defined(_WIN64)
    EXPECT_TRUE(DefaultSigpipe::is_default());
#endif
}

// The server writing to a client that already closed gets a failed send, not SIGPIPE
TEST_F(TCPSigpipeTest, ServerWriteToClosedPeerDoesNotRaiseSigpipe) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    ASSERT_TRUE(client_->send_data(std::string("send-late")));
    client_->disconnect();
    ASSERT_TRUE(waitForCondition([this]() { return server_->disconnected_clients.load() == 1; }));
    EXPECT_TRUE(server_->late_send_failed.load());
}

// The client writing to a server that already closed the connection gets a failed send, not SIGPIPE
TEST_F(TCPSigpipeTest, ClientWriteToClosedPeerDoesNotRaiseSigpipe) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    ASSERT_TRUE(client_->send_data(std::string("bye")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->disconnected_count.load() == 1; }));
    EXPECT_TRUE(client_->late_send_failed.load());
    EXPECT_FALSE(client_->is_connected());
}

// send_data() on other threads must never use a socket that disconnect() is closing, or that a
// reconnect has replaced. Also run under ThreadSanitizer to catch unsynchronized access to the handle.
TEST_F(TCPIntegrationTest, SendConcurrentWithDisconnect) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    server_->echo = false;
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    std::atomic<bool> done{false};
    std::atomic<int> sends_ok{0};
    std::vector<std::thread> senders;
    for (int i = 0; i < 4; ++i) {
        senders.emplace_back([&]() {
            while (!done) {
                if (client_->send_data(std::string("payload"))) {
                    sends_ok++;
                } else {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));  // between connections
                }
            }
        });
    }

    constexpr int reconnects = 20;
    int reconnected = 0;
    for (int i = 0; i < reconnects; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        client_->disconnect();
        if (!client_->connect()) {
            break;
        }
        reconnected++;
    }
    done = true;
    for (auto& t : senders) {
        t.join();
    }

    EXPECT_EQ(reconnected, reconnects);
    EXPECT_GT(sends_ok.load(), 0);
    EXPECT_TRUE(client_->is_connected());
    EXPECT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == reconnects + 1; }));
}

// connect() cannot run on the client thread (it would have to join itself), so it fails from
// onDisconnected(); reconnecting from another thread afterwards works
TEST_F(TCPIntegrationTest, ReconnectMustHappenOutsideClientCallbacks) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    client_->connect_in_on_disconnected = true;
    ASSERT_TRUE(client_->send_data(std::string("kick")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->disconnected_count.load() == 1; }));
    EXPECT_EQ(client_->callback_connect_result.load(), 0);
    EXPECT_FALSE(client_->is_connected());

    client_->connect_in_on_disconnected = false;
    ASSERT_TRUE(client_->connect());
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 2; }));
}

// onConnected() completes before any onData(): the caller's post-connect setup must not race data from
// a server that sends the moment the connection is accepted
TEST_F(TCPIntegrationTest, OnConnectedCompletesBeforeOnData) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    server_->greet_on_connect = true;

    client_ = std::make_unique<IntegrationTestClient>("IntegrationClient", client_config_);
    client_->slow_on_connected = true;
    ASSERT_TRUE(client_->connect());
    ASSERT_TRUE(waitForCondition([this]() { return client_->received_data() == "hello"; }));
    EXPECT_FALSE(client_->data_before_on_connected.load());
}

// disconnect() from onConnected() ends the connection before the client thread exists, so it reports
// onDisconnected() itself, and connect() returns false
TEST_F(TCPIntegrationTest, DisconnectFromOnConnected) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = std::make_unique<IntegrationTestClient>("IntegrationClient", client_config_);
    client_->disconnect_in_on_connected = true;

    EXPECT_FALSE(client_->connect());
    EXPECT_FALSE(client_->is_connected());
    EXPECT_EQ(client_->connected_count.load(), 1);
    EXPECT_EQ(client_->disconnected_count.load(), 1);
    ASSERT_TRUE(waitForCondition([this]() { return server_->disconnected_clients.load() == 1; }));

    // The client is reusable afterwards
    ASSERT_TRUE(client_->connect());
    ASSERT_TRUE(client_->send_data(std::string("again")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->received_data() == "again"; }));
}

// disconnect() then connect() from onConnected(): the nested connect() starts the client thread and the
// outer one must not start a second; each connection is reported once
TEST_F(TCPIntegrationTest, ReconnectFromOnConnected) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = std::make_unique<IntegrationTestClient>("IntegrationClient", client_config_);
    client_->reconnect_in_on_connected = true;

    EXPECT_TRUE(client_->connect());
    EXPECT_TRUE(client_->is_connected());
    EXPECT_EQ(client_->connected_count.load(), 2);
    EXPECT_EQ(client_->disconnected_count.load(), 1);
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 2; }));
    ASSERT_TRUE(client_->send_data(std::string("still works")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->received_data() == "still works"; }));
}

// Destroying a connected client must not call onDisconnected() on the already-destroyed derived object
TEST_F(TCPIntegrationTest, DestroyConnectedClientSkipsDisconnectCallback) {
    ASSERT_NO_FATAL_FAILURE(startServer());

    std::atomic<int> disconnects{0};
    client_ = connectClient("IntegrationClient");
    client_->external_disconnects = &disconnects;
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    client_.reset();
    EXPECT_EQ(disconnects.load(), 0);
    ASSERT_TRUE(waitForCondition([this]() { return server_->disconnected_clients.load() == 1; }));

    // Destroying a connected client breaks the contract and is reported
    expected_unsafe_destroys_ = 1;
}

// Destroying a running server without stop() is reported
TEST_F(TCPIntegrationTest, DestroyRunningServerIsReported) {
    ASSERT_NO_FATAL_FAILURE(startServer());
    server_.reset();
    expected_unsafe_destroys_ = 1;

    // A stopped server and a disconnected client are fine to destroy
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));
    client_->disconnect();
    client_.reset();
    server_->stop();
    server_.reset();
    EXPECT_EQ(g_unsafe_destroys.load(), 1);
}

// get_connected_client_count() is read here while the server thread adds and removes clients
TEST_F(TCPIntegrationTest, ConnectedClientCountFromAnotherThread) {
    ASSERT_NO_FATAL_FAILURE(startServer());

    std::vector<std::unique_ptr<IntegrationTestClient>> clients;
    for (int i = 0; i < 3; ++i) {
        clients.push_back(connectClient("Client" + std::to_string(i)));
    }
    ASSERT_TRUE(waitForCondition([this]() { return server_->get_connected_client_count() == 3; }));

    clients[0]->disconnect();
    ASSERT_TRUE(waitForCondition([this]() { return server_->get_connected_client_count() == 2; }));

    server_->stop();
    EXPECT_EQ(server_->get_connected_client_count(), 0u);

    for (auto& client : clients) {
        EXPECT_TRUE(waitForCondition([&client]() { return !client->is_connected(); }));
        client->disconnect();
    }
}

// An idle connected client must block in poll() rather than spin on recv()
TEST_F(TCPIntegrationTest, IdleClientDoesNotSpinClientThread) {
#if defined(__APPLE__)
    GTEST_SKIP() << "Per-thread CPU time is not measured on macOS";
#endif
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    expect_mostly_idle(client_->client_thread(), "idle client thread");

    // Still receives promptly after idling
    ASSERT_TRUE(client_->send_data(std::string("wake")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->received_data() == "wake"; }));
}

// A client that stops reading must not stall the server thread for everyone else
TEST_F(TCPIntegrationTest, SlowReaderDoesNotBlockServer) {
    server_config_.max_pending_send_bytes = 64 * 1024 * 1024;
    ASSERT_NO_FATAL_FAILURE(startServer());

    auto slow = connectClient("SlowClient");
    client_ = connectClient("FastClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 2; }));

    const auto payload = make_payload(32 * 1024 * 1024);
    server_->flood_payload = payload;
    slow->hold = true;
    ASSERT_TRUE(slow->send_data(std::string("flood")));

    // With a blocking send the server thread would spin until the slow client reads again
    EXPECT_TRUE(waitForCondition([this]() { return server_->flood_done.load(); }, 5000));
    EXPECT_EQ(server_->send_failures.load(), 0);
    ASSERT_TRUE(client_->send_data(std::string("ping")));
    EXPECT_TRUE(waitForCondition([this]() { return client_->received_data() == "ping"; }, 3000));

    // Once the slow client reads again, the queued data arrives complete and in order
    slow->hold = false;
    ASSERT_TRUE(waitForCondition([&]() { return slow->bytes_received.load() == payload.size(); }, 20000))
        << "slow client received " << slow->bytes_received.load() << " of " << payload.size() << " bytes";
    EXPECT_TRUE(slow->received_data() == std::string(payload.begin(), payload.end()));

    slow->disconnect();
}

// send_data() rejects whole messages once a client's queue reaches max_pending_send_bytes
TEST_F(TCPIntegrationTest, PendingSendLimitRejectsMessages) {
    server_config_.max_pending_send_bytes = 1024 * 1024;
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("SlowClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    const auto payload = make_payload(32 * 1024 * 1024);
    server_->flood_payload = payload;
    client_->hold = true;
    ASSERT_TRUE(client_->send_data(std::string("flood")));
    ASSERT_TRUE(waitForCondition([this]() { return server_->flood_done.load(); }, 5000));
    EXPECT_GT(server_->send_failures.load(), 0);
    EXPECT_LT(server_->flood_accepted.load(), payload.size());

    // Rejected messages are dropped whole, so the client sees an intact prefix of the stream
    client_->hold = false;
    const size_t accepted = server_->flood_accepted.load();
    ASSERT_TRUE(waitForCondition([&]() { return client_->bytes_received.load() == accepted; }, 10000));
    EXPECT_TRUE(client_->received_data() == std::string(payload.begin(), payload.begin() + accepted));
    EXPECT_TRUE(client_->is_connected());
}

// A first message larger than max_pending_send_bytes is rejected whole even though the queue is
// empty; a partial direct write would otherwise queue the remainder past the limit
TEST_F(TCPIntegrationTest, PendingSendLimitRejectsOversizedFirstMessage) {
    server_config_.max_pending_send_bytes = 1024 * 1024;
    ASSERT_NO_FATAL_FAILURE(startServer());
    client_ = connectClient("SlowClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    const auto payload = make_payload(32 * 1024 * 1024);
    server_->flood_payload = payload;
    server_->flood_chunk = payload.size();  // one message, 32x the limit
    client_->hold = true;
    ASSERT_TRUE(client_->send_data(std::string("flood")));
    ASSERT_TRUE(waitForCondition([this]() { return server_->flood_done.load(); }, 5000));
    EXPECT_EQ(server_->send_failures.load(), 1);
    EXPECT_EQ(server_->flood_accepted.load(), 0u);

    // Nothing of the rejected message reaches the client, and messages within the limit still go through
    client_->hold = false;
    ASSERT_TRUE(client_->send_data(std::string("ping")));
    ASSERT_TRUE(waitForCondition([this]() { return client_->received_data() == "ping"; }, 3000));
    EXPECT_EQ(client_->bytes_received.load(), 4u);
    EXPECT_TRUE(client_->is_connected());
}

// A client sending into a stalled server must wait in poll() rather than spin on send()
TEST_F(TCPIntegrationTest, ClientSendDoesNotSpinWhenServerStalls) {
#if defined(__APPLE__)
    GTEST_SKIP() << "Per-thread CPU time is not measured on macOS";
#endif
    ASSERT_NO_FATAL_FAILURE(startServer());
    server_->echo = false;
    client_ = connectClient("IntegrationClient");
    ASSERT_TRUE(waitForCondition([this]() { return server_->connected_clients.load() == 1; }));

    server_->hold = true;

    // Send 1 MiB messages until the socket buffers fill and send_data() blocks. The OS may accept
    // a lot before that (Windows loopback takes a whole 32 MiB send at once), so loop until it stalls.
    const auto message = make_payload(1024 * 1024);
    constexpr int max_messages = 256;
    std::atomic<int> messages_sent{0};
    std::atomic<bool> send_failed{false};
    std::thread sender([&]() {
        for (int i = 0; i < max_messages; ++i) {
            if (!client_->send_data(message)) {
                send_failed = true;
                return;
            }
            messages_sent++;
        }
    });

    // Blocked once the count stops moving
    int last = -1;
    ASSERT_TRUE(waitForCondition([&]() {
        const int now = messages_sent.load();
        const bool stalled = now == last;
        last = now;
        if (!stalled) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return stalled;
    }, 10000));
    ASSERT_LT(messages_sent.load(), max_messages) << "send_data() never blocked";
    expect_mostly_idle(sender, "blocked sender thread");

    server_->hold = false;
    sender.join();
    EXPECT_FALSE(send_failed.load());
    EXPECT_EQ(messages_sent.load(), max_messages);
    ASSERT_TRUE(waitForCondition([&]() {
        return server_->bytes_received.load() == static_cast<size_t>(max_messages) * message.size();
    }, 20000));
}

// A server that declares onPoll(): pushes a queued message to a client with no client traffic to
// trigger it, which is what a gateway draining another thread's queue needs.
class PollingServer : public slick::socket::TCPServerBase<PollingServer>
{
public:
    using slick::socket::TCPServerBase<PollingServer>::TCPServerBase;

    void onClientConnected(int client_id, const std::string&) { client_id_ = client_id; }
    void onClientDisconnected(int) {}
    void onClientData(int, const uint8_t*, size_t) {}

    void onPoll() {
        polls++;
        if (poll_thread_id_ == std::thread::id{}) {
            poll_thread_id_ = std::this_thread::get_id();
        }
        if (push_requested.exchange(false)) {
            static constexpr uint8_t payload[] = {'p', 'o', 'l', 'l', 'e', 'd'};
            push_sent = send_data(client_id_.load(), payload, sizeof(payload));
        }
    }

    std::thread::id poll_thread_id() const { return poll_thread_id_; }
    std::thread::id server_thread_id() const { return server_thread_.get_id(); }

    std::atomic<int> client_id_{-1};
    std::atomic<uint64_t> polls{0};
    std::atomic<bool> push_requested{false};
    std::atomic<bool> push_sent{false};

private:
    std::atomic<std::thread::id> poll_thread_id_{};
};

TEST_F(TCPIntegrationTest, OnPollRunsOnServerThreadAndCanSend) {
    auto server = std::make_unique<PollingServer>("PollingServer", server_config_);
    ASSERT_TRUE(server->start());
    client_config_.server_port = server->port();

    // Called with no traffic at all
    ASSERT_TRUE(waitForCondition([&]() { return server->polls.load() > 10; }));
    EXPECT_EQ(server->poll_thread_id(), server->server_thread_id());

    client_ = connectClient("PollClient");
    ASSERT_TRUE(waitForCondition([&]() { return server->client_id_.load() != -1; }));

    // The client never sends anything: the data originates in onPoll()
    server->push_requested = true;
    ASSERT_TRUE(waitForCondition([&]() { return client_->received_data() == "polled"; }));
    EXPECT_TRUE(server->push_sent.load());

    client_->disconnect();
    server->stop();
}

