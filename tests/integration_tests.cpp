#include <gtest/gtest.h>
#include "../examples/logger.h"
#include <slick/socket/tcp_server.h>
#include <slick/socket/tcp_client.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#else
#include <pthread.h>
#include <time.h>
#endif

class IntegrationTestServer : public slick::socket::TCPServerBase<IntegrationTestServer>
{
public:
    using slick::socket::TCPServerBase<IntegrationTestServer>::TCPServerBase;
    using slick::socket::TCPServerBase<IntegrationTestServer>::get_connected_client_count;
    using slick::socket::TCPServerBase<IntegrationTestServer>::send_data;

    void onClientConnected(int client_id, const std::string& client_address) {
        connected_clients++;
        last_connected_client_id = client_id;
    }

    void onClientDisconnected(int client_id) {
        disconnected_clients++;
        last_disconnected_client_id = client_id;
    }

    void onClientData(int client_id, const uint8_t* data, size_t length) {
        if (stop_on_data) {
            stop();  // stopping from within a callback must not self-join the server thread
            return;
        }
        data_received++;
        bytes_received += length;
        last_data_client_id = client_id;
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            last_received_data_.assign((const char*)data, length);
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

    std::atomic<int> connected_clients{0};
    std::atomic<int> disconnected_clients{0};
    std::atomic<int> data_received{0};
    std::atomic<size_t> bytes_received{0};
    std::atomic<bool> stop_on_data{false};
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
        connected_count++;
        connection_established = true;
    }

    void onDisconnected() {
        disconnected_count++;
        connection_established = false;
        if (external_disconnects) {
            (*external_disconnects)++;
        }
    }

    void onData(const uint8_t* data, size_t length) {
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

    std::atomic<int> connected_count{0};
    std::atomic<int> disconnected_count{0};
    std::atomic<int> data_received_count{0};
    std::atomic<size_t> bytes_received{0};
    std::atomic<bool> connection_established{false};
    std::atomic<bool> data_received_flag{false};
    std::atomic<int>* external_disconnects = nullptr;  // outlives the client, unlike the members above

private:
    std::mutex data_mutex_;
    std::string received_data_;
};

// CPU time consumed by a thread so far
static std::chrono::nanoseconds thread_cpu_time(std::thread& thread)
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

class TCPIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use port 0 to let the system assign an available port
        server_config_.port = 0;
        server_config_.max_connections = 10;
        server_config_.receive_buffer_size = 4096;
        server_config_.connection_timeout = std::chrono::milliseconds(5000);

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

    // Destroying the client with a finished-but-unjoined thread must not call std::terminate
    client_.reset();
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
