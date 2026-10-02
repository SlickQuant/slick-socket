#include <gtest/gtest.h>
#include <slick/socket/send_gate.h>
#include <atomic>
#include <chrono>
#include <future>
#include <optional>
#include <thread>
#include <vector>

using slick::socket::detail::SendGate;
using namespace std::chrono_literals;

// Runs gate.close() on another thread so a test can observe whether it is blocked
static std::future<void> close_async(SendGate& gate)
{
    return std::async(std::launch::async, [&gate]() { gate.close(); });
}

TEST(SendGateTest, StartsClosed) {
    SendGate gate;
    SendGate::Pass pass(gate);
    EXPECT_FALSE(pass);
}

TEST(SendGateTest, AdmitsWhileOpenAndAfterReopen) {
    SendGate gate;
    gate.open();
    { SendGate::Pass pass(gate); EXPECT_TRUE(pass); }
    gate.close();
    { SendGate::Pass pass(gate); EXPECT_FALSE(pass); }
    gate.open();
    { SendGate::Pass pass(gate); EXPECT_TRUE(pass); }
}

// close() must not return while an admitted sender may still be using the socket
TEST(SendGateTest, CloseWaitsForAdmittedSender) {
    SendGate gate;
    gate.open();
    std::optional<SendGate::Pass> pass;
    pass.emplace(gate);
    ASSERT_TRUE(*pass);

    auto closed = close_async(gate);
    EXPECT_EQ(closed.wait_for(100ms), std::future_status::timeout);

    pass.reset();
    EXPECT_EQ(closed.wait_for(5s), std::future_status::ready);
}

// A sender turned away by a closed gate must not hold up close(); otherwise callers that keep
// trying to send while the gate is closed could stall stop()/disconnect() indefinitely
TEST(SendGateTest, RejectedSenderDoesNotDelayClose) {
    SendGate gate;
    gate.open();
    gate.close();

    std::optional<SendGate::Pass> rejected;
    rejected.emplace(gate);
    ASSERT_FALSE(*rejected);

    auto closed = close_async(gate);
    const bool returned = closed.wait_for(5s) == std::future_status::ready;
    rejected.reset();  // unblocks a close() that wrongly waited, so the test cannot hang
    closed.wait();
    EXPECT_TRUE(returned);
}

// Threads hammering the gate without pausing must neither starve close() nor be admitted while closed
TEST(SendGateTest, CloseCompletesUnderSustainedSendAttempts) {
    SendGate gate;
    std::atomic<bool> closed_for_sure{false};  // set only between close() returning and the next open()
    std::atomic<bool> admitted_while_closed{false};
    std::atomic<bool> done{false};
    std::vector<std::thread> senders;
    for (int i = 0; i < 4; ++i) {
        senders.emplace_back([&]() {
            while (!done.load(std::memory_order_relaxed)) {
                SendGate::Pass pass(gate);
                if (pass && closed_for_sure.load(std::memory_order_relaxed)) {
                    admitted_while_closed = true;
                }
            }
        });
    }

    auto cycles = std::async(std::launch::async, [&]() {
        for (int i = 0; i < 2000; ++i) {
            gate.open();
            std::this_thread::yield();
            gate.close();
            closed_for_sure = true;
            std::this_thread::yield();
            closed_for_sure = false;
        }
    });
    const bool finished = cycles.wait_for(30s) == std::future_status::ready;
    done = true;
    for (auto& t : senders) {
        t.join();
    }
    ASSERT_TRUE(finished) << "close() was starved by rejected send attempts";
    cycles.get();
    EXPECT_FALSE(admitted_while_closed.load());
}
