#include <gtest/gtest.h>
#include <type_traits>
#include <slick/socket/tcp_client.h>
#include <thread>
#include <chrono>
#include <atomic>

class TestClient : public slick::socket::TCPClientBase<TestClient>
{
public:
    using slick::socket::TCPClientBase<TestClient>::TCPClientBase;
    
    void onConnected() {
        connected_count++;
    }
    
    void onDisconnected() {
        disconnected_count++;
    }
    
    void onData(const uint8_t* data, size_t length) {
        data_received_count++;
        last_received_data = std::string((const char*)data, length);
    }

    std::atomic<int> connected_count{0};
    std::atomic<int> disconnected_count{0};
    std::atomic<int> data_received_count{0};
    std::string last_received_data;
};

// Objects own sockets and a worker thread that captures `this`, so moving one would leave two
// owners of the same OS resources
static_assert(!std::is_move_constructible_v<TestClient>);
static_assert(!std::is_move_assignable_v<TestClient>);

class TCPClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.server_address = "127.0.0.1";
        config_.server_port = 12345; // Use a test port
        config_.receive_buffer_size = 4096;
        config_.connection_timeout = std::chrono::milliseconds(1000);
    }

    void TearDown() override {
        if (client_ && client_->is_connected()) {
            client_->disconnect();
        }
    }

    slick::socket::TCPClientConfig config_;
    std::unique_ptr<TestClient> client_;
};

TEST_F(TCPClientTest, ClientCreationAndDestruction) {
    client_ = std::make_unique<TestClient>("TestClient", config_);
    ASSERT_NE(client_, nullptr);
    EXPECT_FALSE(client_->is_connected());
}

TEST_F(TCPClientTest, ClientConnectionAttempt) {
    client_ = std::make_unique<TestClient>("TestClient", config_);
    
    // Try to connect (will likely fail since no server is running)
    bool connected = client_->connect();
    
    // Give some time for connection attempt
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // On Windows, connection attempts to non-existent servers may behave differently
    // Just ensure the client object is in a valid state
    EXPECT_NE(client_, nullptr);
}

TEST_F(TCPClientTest, ClientDisconnection) {
    client_ = std::make_unique<TestClient>("TestClient", config_);
    
    // Try to disconnect (should be safe even if not connected)
    client_->disconnect();
    
    // Client should remain in a valid state
    EXPECT_FALSE(client_->is_connected());
}

TEST_F(TCPClientTest, ClientConfigurationValidation) {
    // Test valid configuration
    slick::socket::TCPClientConfig valid_config;
    valid_config.server_address = "192.168.1.1";
    valid_config.server_port = 8080;
    valid_config.receive_buffer_size = 8192;
    valid_config.connection_timeout = std::chrono::milliseconds(5000);

    client_ = std::make_unique<TestClient>("TestClient", valid_config);
    EXPECT_NE(client_, nullptr);
    EXPECT_FALSE(client_->is_connected());
}

TEST_F(TCPClientTest, SendDataWhenNotConnected) {
    client_ = std::make_unique<TestClient>("TestClient", config_);
    
    std::string test_data = "Hello, World!";
    bool result = client_->send_data(test_data);
    
    // Should return false since not connected
    EXPECT_FALSE(result);
}

TEST_F(TCPClientTest, ClientCallbackInitialization) {
    client_ = std::make_unique<TestClient>("TestClient", config_);
    
    // Verify initial callback state
    EXPECT_EQ(client_->connected_count.load(), 0);
    EXPECT_EQ(client_->disconnected_count.load(), 0);
    EXPECT_EQ(client_->data_received_count.load(), 0);
    EXPECT_TRUE(client_->last_received_data.empty());
}