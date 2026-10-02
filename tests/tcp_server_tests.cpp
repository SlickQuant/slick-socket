#include <gtest/gtest.h>
#include <type_traits>
#include <slick/socket/tcp_server.h>
#include <thread>
#include <chrono>

class TestServer : public slick::socket::TCPServerBase<TestServer>
{
public:
    using slick::socket::TCPServerBase<TestServer>::TCPServerBase;
    using slick::socket::TCPServerBase<TestServer>::get_connected_client_count; // Make public for testing
    
    void onClientConnected(int, const std::string&) {
        connected_clients++;
    }
    
    void onClientDisconnected(int) {
        disconnected_clients++;
    }
    
    void onClientData(int, const uint8_t*, size_t) {
        data_received++;
    }

    std::atomic<int> connected_clients{0};
    std::atomic<int> disconnected_clients{0};
    std::atomic<int> data_received{0};
};

// Objects own sockets and a worker thread that captures `this`, so moving one would leave two
// owners of the same OS resources
static_assert(!std::is_move_constructible_v<TestServer>);
static_assert(!std::is_move_assignable_v<TestServer>);

class TCPServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.port = 0; // Use any available port
        config_.max_connections = 10;
        config_.receive_buffer_size = 4096;
    }

    void TearDown() override {
        if (server_ && server_->is_running()) {
            server_->stop();
        }
    }

    slick::socket::TCPServerConfig config_;
    std::unique_ptr<TestServer> server_;
};

TEST_F(TCPServerTest, ServerCreationAndDestruction) {
    server_ = std::make_unique<TestServer>("TestServer", config_);
    ASSERT_NE(server_, nullptr);
    EXPECT_FALSE(server_->is_running());
}

TEST_F(TCPServerTest, ServerStartAndStop) {
    server_ = std::make_unique<TestServer>("TestServer", config_);
    
    // Test server start
    EXPECT_TRUE(server_->start());
    
    // Give the server some time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(server_->is_running());
    
    // Test server stop
    server_->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(server_->is_running());
}

TEST_F(TCPServerTest, ServerStatistics) {
    server_ = std::make_unique<TestServer>("TestServer", config_);
    ASSERT_TRUE(server_->start());
    
    // Give the server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Test initial state
    EXPECT_EQ(server_->get_connected_client_count(), 0u);
    
    server_->stop();
}

TEST_F(TCPServerTest, ConfigurationValidation) {
    // Test valid configuration
    slick::socket::TCPServerConfig valid_config;
    valid_config.port = 8080;
    valid_config.max_connections = 100;
    valid_config.receive_buffer_size = 8192;
    valid_config.idle_timeout = std::chrono::milliseconds(5000);

    server_ = std::make_unique<TestServer>("TestServer", valid_config);
    // Server creation should succeed
    EXPECT_NE(server_, nullptr);
}