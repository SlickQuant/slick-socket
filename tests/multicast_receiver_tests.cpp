#include <gtest/gtest.h>
#include <type_traits>
#include <slick/socket/multicast_receiver.h>
#include <slick/socket/multicast_sender.h>
#include <thread>
#include <chrono>
#include <atomic>

class TestMulticastReceiver : public slick::socket::MulticastReceiverBase<TestMulticastReceiver>
{
public:
    using slick::socket::MulticastReceiverBase<TestMulticastReceiver>::MulticastReceiverBase;
    using slick::socket::MulticastReceiverBase<TestMulticastReceiver>::get_packets_received;
    using slick::socket::MulticastReceiverBase<TestMulticastReceiver>::get_bytes_received;
    using slick::socket::MulticastReceiverBase<TestMulticastReceiver>::get_receive_errors;

    void handle_multicast_data(const std::vector<uint8_t>& data, const std::string& sender_address)
    {
        if (stop_in_callback) {
            stop();  // must not self-join the receiver thread
        }
        data_received_count++;
        last_received_data = std::string(data.begin(), data.end());
        last_sender_address = sender_address;
        data_received_flag.store(true);
    }

    std::atomic<int> data_received_count{0};
    std::atomic<bool> data_received_flag{false};
    std::atomic<bool> stop_in_callback{false};
    std::string last_received_data;
    std::string last_sender_address;
};

// Objects own sockets and a worker thread that captures `this`, so moving one would leave two
// owners of the same OS resources
static_assert(!std::is_move_constructible_v<TestMulticastReceiver>);
static_assert(!std::is_move_assignable_v<TestMulticastReceiver>);

class MulticastReceiverTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.multicast_address = "224.0.0.101"; // Test multicast address
        config_.port = 12346;
        config_.interface_address = "0.0.0.0"; // Any interface
        config_.reuse_address = true;
        config_.receive_buffer_size = 65536;
        config_.receive_timeout = std::chrono::milliseconds(500); // Shorter timeout for tests
    }

    void TearDown() override {
        if (receiver_ && receiver_->is_running()) {
            receiver_->stop();
        }
    }

    slick::socket::MulticastReceiverConfig config_;
    std::unique_ptr<TestMulticastReceiver> receiver_;
};

TEST_F(MulticastReceiverTest, ReceiverCreationAndDestruction) {
    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", config_);
    ASSERT_NE(receiver_, nullptr);
    EXPECT_FALSE(receiver_->is_running());
}

TEST_F(MulticastReceiverTest, ReceiverStartAndStop) {
    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", config_);
    
    // Test receiver start
    EXPECT_TRUE(receiver_->start());
    
    // Give the receiver some time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(receiver_->is_running());
    
    // Test receiver stop
    receiver_->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(receiver_->is_running());
}

TEST_F(MulticastReceiverTest, ReceiverStatistics) {
    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", config_);
    ASSERT_TRUE(receiver_->start());
    
    // Give the receiver time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Test initial statistics
    EXPECT_EQ(receiver_->get_packets_received(), 0u);
    EXPECT_EQ(receiver_->get_bytes_received(), 0u);
    EXPECT_EQ(receiver_->get_receive_errors(), 0u);
    
    receiver_->stop();
}

TEST_F(MulticastReceiverTest, ReceiverCallbackInitialization) {
    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", config_);
    
    // Verify initial callback state
    EXPECT_EQ(receiver_->data_received_count.load(), 0);
    EXPECT_FALSE(receiver_->data_received_flag.load());
    EXPECT_TRUE(receiver_->last_received_data.empty());
    EXPECT_TRUE(receiver_->last_sender_address.empty());
}

TEST_F(MulticastReceiverTest, ConfigurationValidation) {
    // Test valid configuration
    slick::socket::MulticastReceiverConfig valid_config;
    valid_config.multicast_address = "224.1.1.2";
    valid_config.port = 9998;
    valid_config.reuse_address = false;
    valid_config.receive_timeout = std::chrono::milliseconds(2000);

    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", valid_config);
    EXPECT_NE(receiver_, nullptr);
    EXPECT_FALSE(receiver_->is_running());
    
    // Should be able to start
    EXPECT_TRUE(receiver_->start());
    receiver_->stop();
}

TEST_F(MulticastReceiverTest, ReceiverRestart) {
    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", config_);
    
    // Start receiver
    EXPECT_TRUE(receiver_->start());
    EXPECT_TRUE(receiver_->is_running());
    
    // Stop receiver
    receiver_->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(receiver_->is_running());
    
    // Restart receiver
    EXPECT_TRUE(receiver_->start());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(receiver_->is_running());
    
    receiver_->stop();
}

TEST_F(MulticastReceiverTest, InvalidMulticastAddress) {
    // Test with invalid multicast address
    config_.multicast_address = "invalid.address";
    
    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", config_);
    
    // Start should fail due to invalid address
    EXPECT_FALSE(receiver_->start());
    EXPECT_FALSE(receiver_->is_running());
}

TEST_F(MulticastReceiverTest, MultipleReceiversSameGroup) {
    // Test that multiple receivers can listen to the same multicast group
    auto receiver1 = std::make_unique<TestMulticastReceiver>("TestReceiver1", config_);
    auto receiver2 = std::make_unique<TestMulticastReceiver>("TestReceiver2", config_);

    // Both should be able to start (due to reuse_address = true and SO_REUSEPORT)
    EXPECT_TRUE(receiver1->start());
    EXPECT_TRUE(receiver2->start());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_TRUE(receiver1->is_running());
    EXPECT_TRUE(receiver2->is_running());

    receiver1->stop();
    receiver2->stop();
}

TEST_F(MulticastReceiverTest, ReceiverTimeout) {
    // Test that receiver handles timeouts correctly
    config_.receive_timeout = std::chrono::milliseconds(100); // Very short timeout
    
    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", config_);
    EXPECT_TRUE(receiver_->start());
    
    std::this_thread::sleep_for(std::chrono::milliseconds(300)); // Wait longer than timeout
    
    // Should still be running despite timeouts
    EXPECT_TRUE(receiver_->is_running());
    
    // Should not have received any data
    EXPECT_EQ(receiver_->data_received_count.load(), 0);
    
    receiver_->stop();
}

TEST_F(MulticastReceiverTest, ReceiverPortBinding) {
    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", config_);
    
    // Should bind to the specified port successfully
    EXPECT_TRUE(receiver_->start());
    EXPECT_TRUE(receiver_->is_running());
    
    receiver_->stop();
    
    // Should be able to restart and bind to the same port again
    EXPECT_TRUE(receiver_->start());
    EXPECT_TRUE(receiver_->is_running());
    
    receiver_->stop();
}
TEST_F(MulticastReceiverTest, StopFromReceiveCallback) {
    config_.multicast_address = "224.0.0.102";
    config_.port = 12347;
    config_.receive_timeout = std::chrono::milliseconds(100);
    receiver_ = std::make_unique<TestMulticastReceiver>("TestMulticastReceiver", config_);
    receiver_->stop_in_callback = true;
    ASSERT_TRUE(receiver_->start());

    slick::socket::MulticastSenderConfig sender_config;
    sender_config.multicast_address = config_.multicast_address;
    sender_config.port = config_.port;
    sender_config.enable_loopback = true;
    slick::socket::MulticastSender sender("TestMulticastSender", sender_config);
    ASSERT_TRUE(sender.start());

    // Keep sending until the callback runs (multicast loopback may be unavailable)
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (receiver_->data_received_count.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        sender.send_data(std::string("stop"));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (receiver_->data_received_count.load() == 0) {
        GTEST_SKIP() << "Multicast loopback not available in this environment";
    }

    EXPECT_FALSE(receiver_->is_running());
    EXPECT_EQ(receiver_->data_received_count.load(), 1);

    // Restart joins the finished thread and receives again
    receiver_->stop_in_callback = false;
    ASSERT_TRUE(receiver_->start());
    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (receiver_->data_received_count.load() < 2 && std::chrono::steady_clock::now() < deadline) {
        sender.send_data(std::string("again"));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_GE(receiver_->data_received_count.load(), 2);
    receiver_->stop();
    sender.stop();
}
