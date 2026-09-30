#include <gtest/gtest.h>
#include <type_traits>
#include <slick/socket/multicast_sender.h>
#include <thread>
#include <chrono>

// The sender owns its socket, so moving one would leave two owners of the same OS resource
static_assert(!std::is_move_constructible_v<slick::socket::MulticastSender>);
static_assert(!std::is_move_assignable_v<slick::socket::MulticastSender>);

class MulticastSenderTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.multicast_address = "224.0.0.100"; // Test multicast address
        config_.port = 12345;
        config_.interface_address = "0.0.0.0"; // Any interface
        config_.ttl = 1; // Local network only for testing
        config_.enable_loopback = true; // Enable loopback for CI environments
        config_.send_buffer_size = 65536;
    }

    void TearDown() override {
        if (sender_ && sender_->is_running()) {
            sender_->stop();
        }
    }

    slick::socket::MulticastSenderConfig config_;
    std::unique_ptr<slick::socket::MulticastSender> sender_;
};

TEST_F(MulticastSenderTest, SenderCreationAndDestruction) {
    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", config_);
    ASSERT_NE(sender_, nullptr);
    EXPECT_FALSE(sender_->is_running());
}

TEST_F(MulticastSenderTest, SenderStartAndStop) {
    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", config_);
    
    // Test sender start
    EXPECT_TRUE(sender_->start());
    
    // Give the sender some time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(sender_->is_running());
    
    // Test sender stop
    sender_->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(sender_->is_running());
}

TEST_F(MulticastSenderTest, SenderStatistics) {
    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", config_);
    ASSERT_TRUE(sender_->start());
    
    // Give the sender time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Test initial statistics
    EXPECT_EQ(sender_->get_packets_sent(), 0u);
    EXPECT_EQ(sender_->get_bytes_sent(), 0u);
    EXPECT_EQ(sender_->get_send_errors(), 0u);
    
    sender_->stop();
}

TEST_F(MulticastSenderTest, SendDataWhenRunning) {
    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", config_);
    ASSERT_TRUE(sender_->start());

    // Give the sender time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Send test data
    std::string test_data = "Hello, Multicast World!";
    bool result = sender_->send_data(test_data);

    // Check if we're in a restricted environment (like GitHub CI)
    // where multicast sending might be blocked
    bool is_ci = std::getenv("CI") != nullptr || std::getenv("GITHUB_ACTIONS") != nullptr;

    if (is_ci && !result) {
        // In CI, multicast may be restricted - skip assertions
        GTEST_SKIP() << "Multicast sending not supported in CI environment";
    }

    // Should succeed in normal environments
    EXPECT_TRUE(result);

    // Give time for statistics to update
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Check statistics
    EXPECT_GT(sender_->get_packets_sent(), 0u);
    EXPECT_GT(sender_->get_bytes_sent(), 0u);

    sender_->stop();
}

TEST_F(MulticastSenderTest, SendDataWhenNotRunning) {
    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", config_);
    
    std::string test_data = "This should fail";
    bool result = sender_->send_data(test_data);
    
    // Should return false since not running
    EXPECT_FALSE(result);
    
    // Statistics should remain zero
    EXPECT_EQ(sender_->get_packets_sent(), 0u);
    EXPECT_EQ(sender_->get_bytes_sent(), 0u);
}

TEST_F(MulticastSenderTest, SendEmptyData) {
    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", config_);
    ASSERT_TRUE(sender_->start());
    
    // Give the sender time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Send empty data
    std::vector<uint8_t> empty_data;
    bool result = sender_->send_data(empty_data);
    
    // Should return false for empty data
    EXPECT_FALSE(result);
    
    sender_->stop();
}

TEST_F(MulticastSenderTest, ConfigurationValidation) {
    // Test valid configuration
    slick::socket::MulticastSenderConfig valid_config;
    valid_config.multicast_address = "224.1.1.1";
    valid_config.port = 9999;
    valid_config.ttl = 5;
    valid_config.enable_loopback = true;

    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", valid_config);
    EXPECT_NE(sender_, nullptr);
    EXPECT_FALSE(sender_->is_running());
    
    // Should be able to start
    EXPECT_TRUE(sender_->start());
    sender_->stop();
}

TEST_F(MulticastSenderTest, MultipleDataSends) {
    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", config_);
    ASSERT_TRUE(sender_->start());

    // Give the sender time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    const int num_sends = 5;
    const std::string base_message = "Message ";

    // Check if we're in a restricted environment
    bool is_ci = std::getenv("CI") != nullptr || std::getenv("GITHUB_ACTIONS") != nullptr;
    int successful_sends = 0;

    // Send multiple messages
    for (int i = 0; i < num_sends; ++i) {
        std::string message = base_message + std::to_string(i);
        bool result = sender_->send_data(message);
        if (result) {
            successful_sends++;
        }
        if (!is_ci) {
            EXPECT_TRUE(result);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (is_ci && successful_sends == 0) {
        sender_->stop();
        GTEST_SKIP() << "Multicast sending not supported in CI environment";
    }

    // Give time for all sends to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Check statistics
    EXPECT_GE(sender_->get_packets_sent(), static_cast<uint64_t>(successful_sends));
    EXPECT_GT(sender_->get_bytes_sent(), 0u);

    sender_->stop();
}

TEST_F(MulticastSenderTest, SenderRestart) {
    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", config_);
    
    // Start sender
    EXPECT_TRUE(sender_->start());
    EXPECT_TRUE(sender_->is_running());
    
    // Stop sender
    sender_->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(sender_->is_running());
    
    // Restart sender
    EXPECT_TRUE(sender_->start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(sender_->is_running());
    
    sender_->stop();
}

TEST_F(MulticastSenderTest, InvalidMulticastAddress) {
    // Test with invalid multicast address
    config_.multicast_address = "invalid.address";
    
    sender_ = std::make_unique<slick::socket::MulticastSender>("TestMulticastSender", config_);
    EXPECT_TRUE(sender_->start()); // Start should succeed
    
    // But sending should fail
    std::string test_data = "This should fail";
    bool result = sender_->send_data(test_data);
    EXPECT_FALSE(result);
    
    // Should have error count
    EXPECT_GT(sender_->get_send_errors(), 0u);
    
    sender_->stop();
}