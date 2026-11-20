// Performance and burst regression tests for the multicast sockets.
//
// Where the unit tests check single datagrams, these run whole feeds. Sustained and bursty feeds must
// lose nothing while they fit the receive buffer, and an overload must not damage the receiver. The
// receive cost per datagram must not grow with receive_buffer_size, and the receive path must not
// allocate per datagram. Each test prints what it measured, so a slowdown shows in the log before it
// trips an assertion.

#include <gtest/gtest.h>
#include <slick/socket/multicast_receiver.h>
#include <slick/socket/multicast_sender.h>
#include <slick/socket/polled_multicast_receiver.h>
#include "allocation_counter.h"
#include "multicast_test_support.h"
#include "thread_cpu_time.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace {

using slick::socket::MulticastReceiverBase;
using slick::socket::MulticastReceiverConfig;
using slick::socket::MulticastSender;
using slick::socket::MulticastSenderConfig;
using slick::socket::PolledMulticastReceiver;
using slick::socket::PolledMulticastReceiverConfig;

// ---------------------------------------------------------------------------
// Datagram format
// ---------------------------------------------------------------------------

// Every datagram the feeds send carries this header, so the receiver can tell a lost datagram from one
// that arrived cut short or damaged, and can check the order within each sender.
constexpr uint32_t kMagic = 0x534C4B50;  // 'S' 'L' 'K' 'P'
constexpr size_t kHeaderSize = 16;
constexpr size_t kMaxDatagram = 1400;  // under a 1500-byte MTU with the IPv4 and UDP headers

// Probes are counted apart from feed datagrams, so the probes sent while waiting for the group join
// don't skew the feed's statistics
constexpr uint16_t kProbeSender = 0xFFFF;

inline void put_u16(uint8_t* destination, uint16_t value)
{
    destination[0] = static_cast<uint8_t>(value);
    destination[1] = static_cast<uint8_t>(value >> 8);
}

inline void put_u32(uint8_t* destination, uint32_t value)
{
    destination[0] = static_cast<uint8_t>(value);
    destination[1] = static_cast<uint8_t>(value >> 8);
    destination[2] = static_cast<uint8_t>(value >> 16);
    destination[3] = static_cast<uint8_t>(value >> 24);
}

inline uint16_t get_u16(const uint8_t* source)
{
    return static_cast<uint16_t>(static_cast<uint32_t>(source[0]) | (static_cast<uint32_t>(source[1]) << 8));
}

inline uint32_t get_u32(const uint8_t* source)
{
    return static_cast<uint32_t>(source[0]) | (static_cast<uint32_t>(source[1]) << 8) |
           (static_cast<uint32_t>(source[2]) << 16) | (static_cast<uint32_t>(source[3]) << 24);
}

// The body of a datagram depends only on its sequence, so a datagram that was cut short or spliced
// fails the check. It costs one multiply-add per byte, which keeps the test harness itself cheap.
inline uint8_t body_byte(uint32_t sequence, size_t index)
{
    return static_cast<uint8_t>(sequence + static_cast<uint32_t>(index) * 7u);
}

// The header's checksum field ties the header's sequence to the body without a pass over the body
inline uint32_t body_checksum(uint32_t sequence)
{
    return sequence * 2654435761u;
}

inline void fill_datagram(uint8_t* datagram, size_t size, uint16_t sender, uint32_t sequence)
{
    assert(size >= kHeaderSize && size <= kMaxDatagram);
    put_u32(datagram, kMagic);
    put_u16(datagram + 4, sender);
    put_u16(datagram + 6, static_cast<uint16_t>(size));
    put_u32(datagram + 8, sequence);
    for (size_t i = kHeaderSize; i < size; ++i)
    {
        datagram[i] = body_byte(sequence, i - kHeaderSize);
    }
    put_u32(datagram + 12, body_checksum(sequence));
}

struct ParsedDatagram
{
    uint16_t sender = kProbeSender;
    uint32_t sequence = 0;
    bool intact = false;
};

inline ParsedDatagram parse_datagram(const uint8_t* datagram, size_t size)
{
    ParsedDatagram parsed;
    if (size < kHeaderSize || get_u32(datagram) != kMagic || get_u16(datagram + 6) != size)
    {
        return parsed;
    }
    const uint32_t sequence = get_u32(datagram + 8);
    if (get_u32(datagram + 12) != body_checksum(sequence))
    {
        return parsed;
    }
    for (size_t i = kHeaderSize; i < size; ++i)
    {
        if (datagram[i] != body_byte(sequence, i - kHeaderSize))
        {
            return parsed;
        }
    }
    parsed.sender = get_u16(datagram + 4);
    parsed.sequence = sequence;
    parsed.intact = true;
    return parsed;
}

// A probe datagram as a string, for SEND_FIRST_OR_SKIP, so the receiver sees a well-formed datagram
std::string probe_datagram()
{
    std::array<uint8_t, kHeaderSize> datagram{};
    fill_datagram(datagram.data(), datagram.size(), kProbeSender, 1);
    return std::string(reinterpret_cast<const char*>(datagram.data()), datagram.size());
}


// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Runs `condition` until it holds or `timeout` passes
bool wait_for(const std::function<bool()>& condition, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!condition())
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

// A receiver that checks every datagram and keeps per-sender sequence bookkeeping, so a test can ask
// exactly what was lost, duplicated or damaged. The callback itself allocates nothing, since it is
// where the receive thread reads the allocation counter.
class BurstReceiver : public MulticastReceiverBase<BurstReceiver>
{
public:
    using MulticastReceiverBase<BurstReceiver>::MulticastReceiverBase;

    // Sizes the per-sender bookkeeping. Call it before start(), so the callback never allocates.
    void track_senders(uint16_t count)
    {
        highest_sequence_.assign(count, 0);
    }

    // The receive loop's thread, so a test can read its CPU time
    std::thread& loop_thread()
    {
        return receiver_thread_;
    }

    void handle_multicast_data(const uint8_t* data, size_t size, const std::string&)
    {
        last_thread_allocations.store(slick::socket::test::allocations_on_this_thread(), std::memory_order_relaxed);
        last_thread_allocation_size.store(slick::socket::test::last_allocation_size(), std::memory_order_relaxed);

        const ParsedDatagram parsed = parse_datagram(data, size);
        if (!parsed.intact)
        {
            corrupt.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        if (parsed.sender == kProbeSender)
        {
            probe.fetch_add(1, std::memory_order_relaxed);
            probe_bytes.fetch_add(static_cast<uint64_t>(size), std::memory_order_relaxed);
            return;
        }
        if (parsed.sender >= highest_sequence_.size())
        {
            corrupt.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        received.fetch_add(1, std::memory_order_relaxed);
        bytes.fetch_add(static_cast<uint64_t>(size), std::memory_order_relaxed);

        const uint32_t highest = highest_sequence_[parsed.sender];
        if (parsed.sequence > highest + 1)
        {
            gaps.fetch_add(parsed.sequence - highest - 1, std::memory_order_relaxed);
        }
        else if (parsed.sequence <= highest)
        {
            duplicates.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        highest_sequence_[parsed.sender] = parsed.sequence;
    }

    // Read it after stop(), which joins the thread that writes it
    uint32_t highest_sequence(uint16_t sender) const
    {
        return highest_sequence_[sender];
    }

    std::atomic<uint64_t> received{0};
    std::atomic<uint64_t> bytes{0};
    std::atomic<uint64_t> gaps{0};
    std::atomic<uint64_t> duplicates{0};
    std::atomic<uint64_t> corrupt{0};
    std::atomic<uint64_t> probe{0};
    std::atomic<uint64_t> probe_bytes{0};
    std::atomic<uint64_t> last_thread_allocations{0};
    std::atomic<uint64_t> last_thread_allocation_size{0};

private:
    std::vector<uint32_t> highest_sequence_;
};

// Sends one probe datagram
void send_probe(MulticastSender& sender, std::array<uint8_t, kHeaderSize>& probe)
{
    fill_datagram(probe.data(), probe.size(), kProbeSender, 1);
    sender.send_data(probe.data(), probe.size());
}

// Probes until the receiver hears one, which means its group join is in effect. Otherwise a feed could
// lose its first datagrams while the join is still in progress.
bool probe_until_heard(MulticastSender& sender, const BurstReceiver& receiver,
                       std::chrono::milliseconds timeout = std::chrono::seconds(5))
{
    std::array<uint8_t, kHeaderSize> probe{};
    const uint64_t before = receiver.probe.load(std::memory_order_relaxed);
    return wait_for(
        [&] {
            send_probe(sender, probe);
            return receiver.probe.load(std::memory_order_relaxed) > before;
        },
        timeout);
}

struct FeedResult
{
    uint64_t sent = 0;
    std::chrono::nanoseconds elapsed{0};
};

// Sends `count` datagrams with at most `window` sent but not yet received, and waits until all arrive or
// `timeout` passes. That keeps the kernel queue far below capacity, so a missing datagram is a real loss
// and not the test outrunning the socket.
bool feed_windowed(MulticastSender& sender, const BurstReceiver& receiver, uint16_t sender_id, uint64_t count,
                   uint64_t window, std::vector<uint8_t>& datagram, std::chrono::milliseconds timeout,
                   FeedResult& result)
{
    const uint64_t baseline = receiver.received.load(std::memory_order_relaxed);
    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + timeout;
    uint64_t sent = 0;
    for (;;)
    {
        const uint64_t delivered = receiver.received.load(std::memory_order_relaxed) - baseline;
        if (delivered >= count || std::chrono::steady_clock::now() >= deadline)
        {
            break;
        }
        while (sent < count)
        {
            const uint64_t delivered_now = receiver.received.load(std::memory_order_relaxed) - baseline;
            const uint64_t in_flight = sent > delivered_now ? sent - delivered_now : 0;
            if (in_flight >= window)
            {
                break;
            }
            fill_datagram(datagram.data(), datagram.size(), sender_id, static_cast<uint32_t>(sent + 1));
            if (!sender.send_data(datagram.data(), datagram.size()))
            {
                result.sent = sent;
                result.elapsed = std::chrono::steady_clock::now() - start;
                return false;
            }
            ++sent;
        }
        std::this_thread::yield();
    }
    result.sent = sent;
    result.elapsed = std::chrono::steady_clock::now() - start;
    return receiver.received.load(std::memory_order_relaxed) - baseline >= count;
}

void report_rate(const char* what, uint64_t datagrams, size_t datagram_size, std::chrono::nanoseconds elapsed)
{
    const double seconds = std::chrono::duration<double>(elapsed).count();
    const double packets_per_second = seconds > 0.0 ? static_cast<double>(datagrams) / seconds : 0.0;
    const double megabytes_per_second = packets_per_second * static_cast<double>(datagram_size) / (1024.0 * 1024.0);
    std::cout << "[ perf ] " << what << ": " << datagrams << " x " << datagram_size << " B in " << std::fixed
              << std::setprecision(3) << seconds << " s = " << static_cast<uint64_t>(packets_per_second)
              << " pkt/s, " << std::setprecision(1) << megabytes_per_second << " MB/s" << std::endl;
}

uint32_t random_between(std::mt19937& generator, uint32_t low, uint32_t high)
{
    std::uniform_int_distribution<uint32_t> distribution(low, high);
    return distribution(generator);
}

// Fixed by default, so a failure can be reproduced. SLICK_SOCKET_PERF_SEED replays a reported one.
uint32_t burst_seed()
{
    if (const char* text = std::getenv("SLICK_SOCKET_PERF_SEED"))
    {
        const long parsed = std::strtol(text, nullptr, 0);
        if (parsed > 0)
        {
            return static_cast<uint32_t>(parsed);
        }
    }
    return 0xC0FFEEu;
}

struct BurstPattern
{
    uint32_t min_datagrams = 8;
    uint32_t max_datagrams = 48;
    uint32_t min_size = 64;
    uint32_t max_size = 1024;
};

struct BurstResult
{
    uint64_t datagrams = 0;
    uint64_t bytes = 0;
    uint64_t bursts = 0;
};


// Sends bursts of random length and datagram size for `duration`. Each burst stays within `budget_bytes`
// of payload and is fully received before the next starts, so nothing can be lost to a queue overflow.
bool run_budgeted_bursts(MulticastSender& sender, const BurstReceiver& receiver, uint16_t sender_id,
                         const BurstPattern& pattern, uint64_t budget_bytes, std::chrono::milliseconds duration,
                         std::mt19937& generator, std::vector<uint8_t>& datagram, BurstResult& result)
{
    const auto deadline = std::chrono::steady_clock::now() + duration;
    while (std::chrono::steady_clock::now() < deadline)
    {
        // Cap the count so a burst of minimum-size datagrams still fits the budget
        const uint32_t fits = static_cast<uint32_t>(std::max<uint64_t>(budget_bytes / pattern.min_size, 1));
        const uint32_t datagrams =
            (std::min)(random_between(generator, pattern.min_datagrams, pattern.max_datagrams), fits);
        const uint64_t baseline = receiver.received.load(std::memory_order_relaxed);

        uint64_t burst_bytes = 0;
        uint32_t sent_in_burst = 0;
        while (sent_in_burst < datagrams)
        {
            const uint64_t remaining = budget_bytes - burst_bytes;
            // Stop once the budget can't fit a minimum-size datagram
            if (remaining < pattern.min_size)
            {
                break;
            }
            const uint32_t size = (std::min)(random_between(generator, pattern.min_size, pattern.max_size),
                                             static_cast<uint32_t>(remaining));
            fill_datagram(datagram.data(), size, sender_id, static_cast<uint32_t>(result.datagrams + 1));
            if (!sender.send_data(datagram.data(), size))
            {
                return false;
            }
            ++sent_in_burst;
            ++result.datagrams;
            burst_bytes += size;
            result.bytes += size;
        }
        if (sent_in_burst == 0)
        {
            return false;  // the budget is smaller than the smallest datagram
        }

        if (!wait_for([&] { return receiver.received.load(std::memory_order_relaxed) >= baseline + sent_in_burst; },
                      std::chrono::seconds(5)))
        {
            return false;
        }
        ++result.bursts;

        if (random_between(generator, 0, 1) != 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    return true;
}

// Sends `datagrams` of random size back to back, more than the receive buffer holds. UDP has no flow
// control, so the kernel drops some of them. The tests check the datagrams that do arrive.
bool run_overload_burst(MulticastSender& sender, const BurstPattern& pattern, uint16_t sender_id, uint64_t datagrams,
                        std::mt19937& generator, std::vector<uint8_t>& datagram, BurstResult& result)
{
    const uint64_t first_sequence = result.datagrams + 1;
    for (uint64_t i = 0; i < datagrams; ++i)
    {
        const uint32_t size = random_between(generator, pattern.min_size, pattern.max_size);
        fill_datagram(datagram.data(), size, sender_id, static_cast<uint32_t>(first_sequence + i));
        if (!sender.send_data(datagram.data(), size))
        {
            return false;
        }
        ++result.datagrams;
        result.bytes += size;
    }
    ++result.bursts;
    return true;
}

// ---------------------------------------------------------------------------
// MulticastReceiverBase: sustained feeds, CPU cost, allocations
// ---------------------------------------------------------------------------

class MulticastReceiverPerformanceTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        receiver_config_.multicast_address = "224.0.0.110";
        receiver_config_.port = 12410;
        receiver_config_.receive_timeout = std::chrono::milliseconds(100);
        receiver_config_.receive_buffer_size = 256 * 1024;

        sender_config_.multicast_address = receiver_config_.multicast_address;
        sender_config_.port = receiver_config_.port;
        sender_config_.enable_loopback = true;
        sender_config_.send_buffer_size = 4 << 20;
    }

    // Checks a feed that should have lost nothing: no gaps, duplicates or damage, and the receiver's
    // counters match what the callback saw
    void expect_nothing_lost(const BurstReceiver& receiver, const MulticastSender& sender, uint64_t datagrams,
                             size_t datagram_size)
    {
        EXPECT_EQ(receiver.gaps.load(), 0u) << "datagrams were lost";
        EXPECT_EQ(receiver.duplicates.load(), 0u);
        EXPECT_EQ(receiver.corrupt.load(), 0u) << "a datagram arrived cut short or corrupted";
        EXPECT_EQ(receiver.received.load(), datagrams);
        EXPECT_EQ(receiver.bytes.load(), datagrams * datagram_size);
        EXPECT_EQ(receiver.get_packets_received(), receiver.received.load() + receiver.probe.load());
        EXPECT_EQ(receiver.get_bytes_received(), receiver.bytes.load() + receiver.probe_bytes.load());
        EXPECT_EQ(receiver.get_receive_errors(), 0u);
        EXPECT_EQ(sender.get_send_errors(), 0u);
    }

    // Receive-loop CPU time per datagram in microseconds, feeding `datagrams` into a receiver with this
    // receive_buffer_size and at most `window` in flight. Returns -1 when the feed didn't complete.
    double receive_cpu_per_datagram(size_t receive_buffer_size, MulticastSender& sender, uint64_t datagrams,
                                    size_t datagram_size, uint64_t window)
    {
        receiver_config_.receive_buffer_size = static_cast<int>(receive_buffer_size);
        BurstReceiver receiver("Receiver", receiver_config_);
        receiver.track_senders(1);
        if (!receiver.start() || !probe_until_heard(sender, receiver))
        {
            receiver.stop();
            return -1.0;
        }

        std::vector<uint8_t> datagram(datagram_size);
        FeedResult result;
        const auto cpu_before = slick::socket::test::thread_cpu_time(receiver.loop_thread());
        const bool arrived =
            feed_windowed(sender, receiver, 0, datagrams, window, datagram, std::chrono::seconds(30), result);
        const auto cpu_used = slick::socket::test::thread_cpu_time(receiver.loop_thread()) - cpu_before;
        receiver.stop();
        return arrived ? static_cast<double>(cpu_used.count()) / 1000.0 / static_cast<double>(datagrams) : -1.0;
    }

    MulticastReceiverConfig receiver_config_;
    MulticastSenderConfig sender_config_;
};

TEST_F(MulticastReceiverPerformanceTest, SustainedFeedLosesNoDatagrams)
{
    constexpr uint64_t kDatagrams = 10000;
    constexpr size_t kDatagramSize = 512;

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    BurstReceiver receiver("Receiver", receiver_config_);
    receiver.track_senders(1);
    ASSERT_TRUE(receiver.start());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram looped back to the receiver";

    std::vector<uint8_t> datagram(kDatagramSize);
    FeedResult result;
    const bool all_arrived =
        feed_windowed(sender, receiver, 0, kDatagrams, 64, datagram, std::chrono::seconds(30), result);
    receiver.stop();
    sender.stop();

    report_rate("sustained feed, 256 KiB receive buffer", kDatagrams, kDatagramSize, result.elapsed);

    ASSERT_TRUE(all_arrived) << "delivered " << receiver.received.load() << " of " << result.sent << " sent, "
                             << receiver.gaps.load() << " missing, " << receiver.corrupt.load() << " corrupt";
    EXPECT_EQ(receiver.highest_sequence(0), kDatagrams);
    expect_nothing_lost(receiver, sender, kDatagrams, kDatagramSize);
}

// The same feed with the 4 MiB receive buffer the README suggests for a caller-driven receiver. A
// buffer sized for a feed must not raise the per-datagram cost.
TEST_F(MulticastReceiverPerformanceTest, LargeReceiveBufferKeepsUp)
{
    constexpr uint64_t kDatagrams = 5000;
    constexpr size_t kDatagramSize = 512;
    receiver_config_.receive_buffer_size = 4 << 20;

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    BurstReceiver receiver("Receiver", receiver_config_);
    receiver.track_senders(1);
    ASSERT_TRUE(receiver.start());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram looped back to the receiver";

    std::vector<uint8_t> datagram(kDatagramSize);
    FeedResult result;
    const bool all_arrived =
        feed_windowed(sender, receiver, 0, kDatagrams, 64, datagram, std::chrono::seconds(30), result);
    receiver.stop();
    sender.stop();

    report_rate("sustained feed, 4 MiB receive buffer", kDatagrams, kDatagramSize, result.elapsed);

    ASSERT_TRUE(all_arrived) << "delivered " << receiver.received.load() << " of " << result.sent << " sent, "
                             << receiver.gaps.load() << " missing, " << receiver.corrupt.load() << " corrupt";
    EXPECT_EQ(receiver.highest_sequence(0), kDatagrams);
    expect_nothing_lost(receiver, sender, kDatagrams, kDatagramSize);
}

// A receive loop that zero-fills or copies its whole buffer per datagram costs time proportional to
// receive_buffer_size. That is what capped receivers at a few thousand datagrams a second and made them
// drop. Comparing the receive thread's own CPU time at two buffer sizes catches it on any machine.
//
// The 80 us margin is wide because Windows measures a thread's CPU time in 15.6 ms ticks, so 2000
// datagrams resolve to about 8 us each, and a busy machine adds noise. A loop that passes over the
// whole buffer per datagram costs hundreds of microseconds at 4 MiB, so the margin still catches it.
TEST_F(MulticastReceiverPerformanceTest, ReceiveCostDoesNotGrowWithTheBuffer)
{
    constexpr uint64_t kDatagrams = 2000;
    constexpr size_t kDatagramSize = 512;
    constexpr size_t kSmallBuffer = 32 * 1024;
    constexpr size_t kLargeBuffer = 4 << 20;
    // Small enough that the datagrams in flight fit even the 32 KiB buffer, so both feeds stay lossless
    // and comparable
    constexpr uint64_t kWindow = 8;

    // Check before starting anything: macOS has no per-thread CPU clock
    std::thread placeholder([] {});
    const bool measurable = slick::socket::test::thread_cpu_time(placeholder).count() >= 0;
    placeholder.join();
    if (!measurable)
    {
        GTEST_SKIP() << "per-thread CPU time isn't available on this platform";
    }

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    const double small_buffer_microseconds =
        receive_cpu_per_datagram(kSmallBuffer, sender, kDatagrams, kDatagramSize, kWindow);
    const double large_buffer_microseconds =
        receive_cpu_per_datagram(kLargeBuffer, sender, kDatagrams, kDatagramSize, kWindow);
    sender.stop();

    ASSERT_GE(small_buffer_microseconds, 0.0) << "the feed with a small receive buffer didn't complete";
    ASSERT_GE(large_buffer_microseconds, 0.0) << "the feed with a large receive buffer didn't complete";

    std::cout << "[ perf ] receive loop CPU: " << std::fixed << std::setprecision(2) << small_buffer_microseconds
              << " us per datagram with a " << (kSmallBuffer / 1024) << " KiB buffer, " << large_buffer_microseconds
              << " us with a " << (kLargeBuffer >> 20) << " MiB buffer" << std::endl;

    EXPECT_LT(large_buffer_microseconds, small_buffer_microseconds + 80.0)
        << "the receive cost grows with receive_buffer_size: " << large_buffer_microseconds
        << " us per datagram at 4 MiB against " << small_buffer_microseconds << " us at 32 KiB";
}

// The receive path must not allocate per datagram. An allocator lock and a free per datagram slow the
// loop enough to turn a burst into drops. The callback runs on the receive thread, so the count it
// reads is that thread's alone.
#if defined(_MSC_VER) && !defined(NDEBUG)
// MSVC's iterator debugging gives every container a heap-allocated proxy, so the std::string the receive
// loop builds for the sender address allocates 16 bytes per datagram here. That measures iterator
// debugging, not the library, so MSVC debug builds skip the check.
TEST_F(MulticastReceiverPerformanceTest, ReceivePathDoesNotAllocatePerDatagram)
{
    GTEST_SKIP() << "MSVC debug builds allocate a container proxy per std::string construction";
}
#else
TEST_F(MulticastReceiverPerformanceTest, ReceivePathDoesNotAllocatePerDatagram)
{
    constexpr uint64_t kDatagrams = 2000;
    constexpr size_t kDatagramSize = 512;

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    BurstReceiver receiver("Receiver", receiver_config_);
    receiver.track_senders(1);
    ASSERT_TRUE(receiver.start());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram looped back to the receiver";

    std::vector<uint8_t> datagram(kDatagramSize);
    FeedResult result;
    slick::socket::test::set_allocation_counting(true);
    const bool all_arrived =
        feed_windowed(sender, receiver, 0, kDatagrams, 64, datagram, std::chrono::seconds(30), result);
    slick::socket::test::set_allocation_counting(false);
    receiver.stop();
    sender.stop();

    ASSERT_TRUE(all_arrived) << "delivered " << receiver.received.load() << " of " << result.sent;
    EXPECT_EQ(receiver.last_thread_allocations.load(), 0u)
        << "the receive loop allocated " << receiver.last_thread_allocations.load() << " times handling "
        << kDatagrams << " datagrams, the last " << receiver.last_thread_allocation_size.load() << " bytes";
    expect_nothing_lost(receiver, sender, kDatagrams, kDatagramSize);
}
#endif


// ---------------------------------------------------------------------------
// MulticastReceiverBase: random bursts
// ---------------------------------------------------------------------------

class MulticastReceiverBurstTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        receiver_config_.multicast_address = "224.0.0.112";
        receiver_config_.port = 12412;
        receiver_config_.receive_timeout = std::chrono::milliseconds(100);
        receiver_config_.receive_buffer_size = 256 * 1024;

        sender_config_.multicast_address = receiver_config_.multicast_address;
        sender_config_.port = receiver_config_.port;
        sender_config_.enable_loopback = true;
        sender_config_.send_buffer_size = 4 << 20;
    }

    MulticastReceiverConfig receiver_config_;
    MulticastSenderConfig sender_config_;
};

// Bursts of random length and datagram size, each small enough for the receive buffer, so every
// datagram must arrive. Market data feeds look like this, spikes separated by lulls.
TEST_F(MulticastReceiverBurstTest, BurstsWithinTheReceiveBudgetAreNotDropped)
{
    constexpr uint64_t kBudgetBytes = 32 * 1024;  // well under the smallest effective SO_RCVBUF
    const uint32_t seed = burst_seed();
    RecordProperty("seed", std::to_string(seed));
    std::mt19937 generator(seed);
    const BurstPattern pattern;

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    BurstReceiver receiver("Receiver", receiver_config_);
    receiver.track_senders(1);
    ASSERT_TRUE(receiver.start());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram looped back to the receiver";

    std::vector<uint8_t> datagram(kMaxDatagram);
    BurstResult result;
    const bool all_arrived = run_budgeted_bursts(sender, receiver, 0, pattern, kBudgetBytes, std::chrono::seconds(1),
                                                  generator, datagram, result);
    receiver.stop();
    sender.stop();

    std::cout << "[ perf ] " << result.bursts << " bursts of " << result.datagrams << " datagrams (" << result.bytes
              << " B) in 1 s, seed " << seed << std::endl;

    ASSERT_TRUE(all_arrived) << "lost datagrams in bursts that fit the receive buffer, seed " << seed;
    EXPECT_EQ(receiver.gaps.load(), 0u) << "datagrams were lost, seed " << seed;
    EXPECT_EQ(receiver.duplicates.load(), 0u);
    EXPECT_EQ(receiver.corrupt.load(), 0u) << "a datagram arrived cut short or corrupted, seed " << seed;
    EXPECT_EQ(receiver.received.load(), result.datagrams);
    EXPECT_EQ(receiver.bytes.load(), result.bytes);
    EXPECT_EQ(receiver.highest_sequence(0), result.datagrams);
    EXPECT_EQ(receiver.get_packets_received(), receiver.received.load() + receiver.probe.load());
    EXPECT_EQ(receiver.get_bytes_received(), receiver.bytes.load() + receiver.probe_bytes.load());
    EXPECT_EQ(receiver.get_receive_errors(), 0u);
    EXPECT_EQ(sender.get_send_errors(), 0u);
}


// The same kind of traffic, but far more than the receive buffer holds. UDP has no flow control, so the
// kernel drops some of it by design. The test fails on a damaged datagram, on counters that disagree
// with what arrived, or if the receiver stops delivering afterwards.
TEST_F(MulticastReceiverBurstTest, BurstsBeyondTheReceiveBudgetStayIntact)
{
    constexpr uint64_t kDatagramsPerBurst = 4096;  // several MiB, many times the 256 KiB receive buffer
    const uint32_t seed = burst_seed();
    RecordProperty("seed", std::to_string(seed));
    std::mt19937 generator(seed);
    BurstPattern pattern;
    pattern.min_size = 512;
    pattern.max_size = 1400;

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    BurstReceiver receiver("Receiver", receiver_config_);
    receiver.track_senders(1);
    ASSERT_TRUE(receiver.start());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram looped back to the receiver";

    std::vector<uint8_t> datagram(kMaxDatagram);
    BurstResult result;
    for (int burst = 0; burst < 2; ++burst)
    {
        ASSERT_TRUE(run_overload_burst(sender, pattern, 0, kDatagramsPerBurst, generator, datagram, result))
            << "a send failed during an overload burst, seed " << seed;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));  // let the loop drain what it kept
    }

    // The receiver must keep delivering after the overload
    const uint64_t probes_before = receiver.probe.load();
    ASSERT_TRUE(wait_for(
                    [&] {
                        std::array<uint8_t, kHeaderSize> probe{};
                        send_probe(sender, probe);
                        return receiver.probe.load() > probes_before;
                    },
                    std::chrono::seconds(5)))
        << "the receiver stopped delivering after an overload, seed " << seed;

    // Wait until the count stops changing, so the totals below are final
    for (uint64_t previous = receiver.received.load();;)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        const uint64_t current = receiver.received.load();
        if (current == previous)
        {
            break;
        }
        previous = current;
    }

    receiver.stop();
    sender.stop();

    const uint64_t received = receiver.received.load();
    const uint64_t duplicates = receiver.duplicates.load();
    const uint64_t gaps = receiver.gaps.load();
    const double delivered_percent = 100.0 * static_cast<double>(received) / static_cast<double>(result.datagrams);
    std::cout << "[ perf ] overload: " << received << " of " << result.datagrams << " delivered (" << std::fixed
              << std::setprecision(2) << delivered_percent << "%), " << gaps << " lost, " << duplicates
              << " duplicated, seed " << seed << std::endl;
    if (gaps == 0)
    {
        std::cout << "[ perf ] note: the receiver kept up with the whole burst, so the buffer was not exceeded"
                  << std::endl;
    }

    EXPECT_EQ(receiver.corrupt.load(), 0u) << "a datagram arrived cut short or corrupted, seed " << seed;
    EXPECT_LE(duplicates, std::max<uint64_t>(result.datagrams / 1000, 1)) << "seed " << seed;  // at most 0.1 %
    EXPECT_EQ(received - duplicates + gaps, receiver.highest_sequence(0))
        << "the counters disagree with the sequences that arrived, seed " << seed;
    // The socket buffer alone holds at least this much, whatever the platform
    EXPECT_GE(received, (64 * 1024) / pattern.max_size) << "the receiver kept almost nothing of the burst";
    EXPECT_EQ(receiver.get_packets_received(), received + receiver.probe.load());
    EXPECT_EQ(receiver.get_bytes_received(), receiver.bytes.load() + receiver.probe_bytes.load());
    EXPECT_EQ(receiver.get_receive_errors(), 0u);
    EXPECT_EQ(sender.get_send_errors(), 0u);
}


// Two threads bursting through one shared MulticastSender, which supports that, with a shared in-flight
// limit so together they can't overflow the socket buffer. Each thread's datagrams must arrive complete
// and in order, and the counters must include both.
TEST_F(MulticastReceiverBurstTest, ConcurrentSendersBurstingStayIntactAndCounted)
{
    constexpr uint16_t kSenders = 2;
    constexpr uint64_t kInFlight = 96;  // datagrams, both threads together
    const uint32_t seed = burst_seed();
    RecordProperty("seed", std::to_string(seed));

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    BurstReceiver receiver("Receiver", receiver_config_);
    receiver.track_senders(kSenders);
    ASSERT_TRUE(receiver.start());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram looped back to the receiver";

    std::atomic<uint64_t> sent{0};
    std::atomic<bool> send_failed{false};
    std::atomic<bool> stalled{false};  // a sender gave up waiting for the receiver to drain
    std::array<uint64_t, kSenders> sent_by_sender{};
    std::array<uint64_t, kSenders> bytes_by_sender{};

    // One thread per sender id, bursting randomly for 600 ms
    const auto burst = [&](uint16_t sender_id) {
        std::mt19937 generator(seed + sender_id);
        std::vector<uint8_t> datagram(kMaxDatagram);
        const BurstPattern pattern;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(600);
        // Waiting for room in the window has to end even if the receiver stops draining, or both threads
        // would spin forever and the join() below would never return
        const auto window_deadline = deadline + std::chrono::seconds(5);
        uint64_t local_sent = 0;
        while (std::chrono::steady_clock::now() < deadline && !send_failed.load() && !stalled.load())
        {
            // Claim a slot in the shared in-flight window
            for (;;)
            {
                if (send_failed.load() || stalled.load())
                {
                    break;
                }
                const uint64_t claimed = sent.load();
                const uint64_t delivered = receiver.received.load();
                if (claimed < delivered || claimed - delivered < kInFlight)
                {
                    break;
                }
                if (std::chrono::steady_clock::now() >= window_deadline)
                {
                    stalled.store(true);
                    break;
                }
                std::this_thread::yield();
            }
            if (send_failed.load() || stalled.load())
            {
                break;
            }
            sent.fetch_add(1);

            const uint32_t size = random_between(generator, pattern.min_size, pattern.max_size);
            ++local_sent;
            bytes_by_sender[sender_id] += size;
            fill_datagram(datagram.data(), size, sender_id, static_cast<uint32_t>(local_sent));
            if (!sender.send_data(datagram.data(), size))
            {
                send_failed.store(true);
                break;
            }
        }
        sent_by_sender[sender_id] = local_sent;
    };

    std::thread first(burst, 0);
    std::thread second(burst, 1);
    first.join();
    second.join();

    const uint64_t total_sent = sent_by_sender[0] + sent_by_sender[1];
    const bool all_arrived =
        wait_for([&] { return receiver.received.load() >= total_sent; }, std::chrono::seconds(10));
    receiver.stop();
    sender.stop();

    std::cout << "[ perf ] " << kSenders << " concurrent senders burst " << total_sent << " datagrams in 600 ms, seed "
              << seed << std::endl;

    ASSERT_FALSE(stalled.load()) << "the receiver stopped draining with " << kInFlight
                                 << " datagrams in flight, seed " << seed;
    ASSERT_TRUE(all_arrived) << "only " << receiver.received.load() << " of " << total_sent
                             << " datagrams arrived, seed " << seed;
    ASSERT_FALSE(send_failed.load()) << "a send failed while both threads were bursting, seed " << seed;
    EXPECT_EQ(receiver.gaps.load(), 0u) << "datagrams were lost, seed " << seed;
    EXPECT_EQ(receiver.duplicates.load(), 0u);
    EXPECT_EQ(receiver.corrupt.load(), 0u) << "a datagram arrived cut short or corrupted, seed " << seed;
    EXPECT_EQ(receiver.received.load(), total_sent);
    EXPECT_EQ(receiver.highest_sequence(0), sent_by_sender[0]);
    EXPECT_EQ(receiver.highest_sequence(1), sent_by_sender[1]);
    EXPECT_EQ(receiver.bytes.load(), bytes_by_sender[0] + bytes_by_sender[1]);
    EXPECT_EQ(receiver.get_packets_received(), receiver.received.load() + receiver.probe.load());
    EXPECT_EQ(receiver.get_bytes_received(), receiver.bytes.load() + receiver.probe_bytes.load());
    EXPECT_EQ(sender.get_send_errors(), 0u);
    EXPECT_GE(sender.get_packets_sent(), total_sent);
}

// ---------------------------------------------------------------------------
// PolledMulticastReceiver: sustained feeds, kernel-queue bursts, allocations
// ---------------------------------------------------------------------------

class PolledMulticastReceiverPerformanceTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        receiver_config_.multicast_address = "224.0.0.111";
        receiver_config_.port = 12411;
        receiver_config_.receive_buffer_size = 1 << 20;

        sender_config_.multicast_address = receiver_config_.multicast_address;
        sender_config_.port = receiver_config_.port;
        sender_config_.enable_loopback = true;  // the receiver is on the sending host
        sender_config_.send_buffer_size = 4 << 20;
    }

    struct DrainResult
    {
        uint64_t datagrams = 0;
        uint64_t bytes = 0;
        uint64_t duplicates = 0;
        uint64_t corrupt = 0;
        uint32_t highest = 0;
    };

    // Reads everything queued, validating each datagram as it goes
    DrainResult drain(PolledMulticastReceiver& receiver, std::span<uint8_t> buffer)
    {
        DrainResult result;
        for (;;)
        {
            const std::ptrdiff_t size = receiver.receive(buffer);
            if (size <= 0)
            {
                break;
            }
            const ParsedDatagram parsed = parse_datagram(buffer.data(), static_cast<size_t>(size));
            if (!parsed.intact)
            {
                ++result.corrupt;
                continue;
            }
            ++result.datagrams;
            result.bytes += static_cast<uint64_t>(size);
            if (parsed.sequence <= result.highest)
            {
                ++result.duplicates;
                continue;
            }
            result.highest = parsed.sequence;
        }
        return result;
    }

    // Probes until the receiver reads one, which is when its group join is in effect, then clears
    // anything still queued so the statistics start from a quiet socket
    bool probe_until_heard(MulticastSender& sender, PolledMulticastReceiver& receiver)
    {
        std::array<uint8_t, kHeaderSize> probe{};
        std::array<uint8_t, 512> buffer{};
        const bool heard = wait_for(
            [&] {
                send_probe(sender, probe);
                return receiver.receive(buffer) > 0;
            },
            std::chrono::seconds(5));
        if (!heard)
        {
            return false;
        }
        wait_for([&] { return receiver.receive(buffer) == 0; }, std::chrono::milliseconds(200));
        return true;
    }

    PolledMulticastReceiverConfig receiver_config_;
    MulticastSenderConfig sender_config_;
};

// One thread sends and reads in turn with at most 64 datagrams unread, the way an application loop
// would use this receiver.
TEST_F(PolledMulticastReceiverPerformanceTest, SustainedFeedLosesNoDatagrams)
{
    constexpr uint64_t kDatagrams = 10000;
    constexpr size_t kDatagramSize = 256;
    constexpr uint64_t kWindow = 64;

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    ASSERT_TRUE(receiver.open());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram reached the receiver";

    const uint64_t packets_before = receiver.get_packets_received();
    const uint64_t bytes_before = receiver.get_bytes_received();

    std::array<uint8_t, 2048> buffer{};
    std::vector<uint8_t> datagram(kDatagramSize);
    uint64_t sent = 0;
    uint64_t received = 0;
    uint64_t duplicates = 0;
    uint64_t corrupt = 0;
    uint32_t highest = 0;
    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + std::chrono::seconds(30);
    while (received < kDatagrams && std::chrono::steady_clock::now() < deadline)
    {
        while (sent < kDatagrams && sent - received < kWindow)
        {
            fill_datagram(datagram.data(), datagram.size(), 0, static_cast<uint32_t>(sent + 1));
            ASSERT_TRUE(sender.send_data(datagram.data(), datagram.size()));
            ++sent;
        }
        const std::ptrdiff_t size = receiver.receive(buffer);
        if (size <= 0)
        {
            std::this_thread::yield();
            continue;
        }
        const ParsedDatagram parsed = parse_datagram(buffer.data(), static_cast<size_t>(size));
        if (!parsed.intact)
        {
            ++corrupt;
            continue;
        }
        ++received;
        if (parsed.sequence <= highest)
        {
            ++duplicates;
            continue;
        }
        highest = parsed.sequence;
    }
    const std::chrono::nanoseconds elapsed = std::chrono::steady_clock::now() - start;
    sender.stop();

    report_rate("polled receiver feed, 256 B datagrams", kDatagrams, kDatagramSize, elapsed);

    EXPECT_EQ(corrupt, 0u) << "a datagram arrived cut short or corrupted";
    EXPECT_EQ(duplicates, 0u);
    EXPECT_EQ(received, kDatagrams) << "read " << received << " of " << sent;
    EXPECT_EQ(receiver.get_packets_received() - packets_before, received);
    EXPECT_EQ(receiver.get_bytes_received() - bytes_before, kDatagrams * kDatagramSize);
    EXPECT_EQ(receiver.get_packets_truncated(), 0u);
    EXPECT_EQ(receiver.get_receive_errors(), 0u);
    EXPECT_EQ(sender.get_send_errors(), 0u);
}

// Nothing reads the socket while the burst goes out, so the kernel queue has to hold all of it. This
// checks that the configured receive_buffer_size reaches the socket.
TEST_F(PolledMulticastReceiverPerformanceTest, BurstWithinTheReceiveBufferSurvives)
{
    constexpr uint64_t kBurst = 200;
    constexpr size_t kDatagramSize = 512;

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    ASSERT_TRUE(receiver.open());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram reached the receiver";

    const uint64_t packets_before = receiver.get_packets_received();
    std::vector<uint8_t> datagram(kDatagramSize);
    for (uint64_t i = 0; i < kBurst; ++i)
    {
        fill_datagram(datagram.data(), datagram.size(), 0, static_cast<uint32_t>(i + 1));
        ASSERT_TRUE(sender.send_data(datagram.data(), datagram.size()));
    }
    sender.stop();

    std::array<uint8_t, 2048> buffer{};
    const DrainResult drained = drain(receiver, buffer);

    std::cout << "[ perf ] burst of " << kBurst << " x " << kDatagramSize << " B, " << drained.datagrams
              << " received" << std::endl;

    EXPECT_EQ(drained.corrupt, 0u) << "a datagram arrived cut short or corrupted";
    EXPECT_EQ(drained.duplicates, 0u);
    EXPECT_EQ(drained.datagrams, kBurst) << "the socket buffer dropped part of a burst that fits it";
    EXPECT_EQ(drained.bytes, kBurst * kDatagramSize);
    EXPECT_EQ(drained.highest, kBurst);
    EXPECT_EQ(receiver.get_packets_received() - packets_before, kBurst);
    EXPECT_EQ(receiver.get_packets_truncated(), 0u);
    EXPECT_EQ(receiver.get_receive_errors(), 0u);
}

// Sends more than the socket buffer holds with nothing reading, so the kernel has to drop some. The
// threaded receiver's loop drains during the burst, but here nothing does, so the drop is certain.
// Datagrams that arrive must be intact, the counters must agree with them, and reading must still work
// afterwards.
TEST_F(PolledMulticastReceiverPerformanceTest, OverloadBurstStaysIntactAndRecovers)
{
    constexpr uint32_t kDatagrams = 4096;
    constexpr size_t kDatagramSize = 1400;  // ~5.6 MiB, many times the 1 MiB receive buffer

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    ASSERT_TRUE(receiver.open());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram reached the receiver";

    const uint64_t packets_before = receiver.get_packets_received();
    std::vector<uint8_t> datagram(kDatagramSize);
    for (uint32_t i = 0; i < kDatagrams; ++i)
    {
        fill_datagram(datagram.data(), datagram.size(), 0, i + 1);
        ASSERT_TRUE(sender.send_data(datagram.data(), datagram.size()));
    }

    std::array<uint8_t, 2048> buffer{};
    const DrainResult drained = drain(receiver, buffer);
    std::cout << "[ perf ] overload burst: " << drained.datagrams << " of " << kDatagrams << " delivered ("
              << std::fixed << std::setprecision(2)
              << 100.0 * static_cast<double>(drained.datagrams) / static_cast<double>(kDatagrams) << "%), "
              << drained.duplicates << " duplicated" << std::endl;

    EXPECT_EQ(drained.corrupt, 0u) << "a datagram arrived cut short or corrupted";
    EXPECT_LE(drained.duplicates, kDatagrams / 1000 + 1);
    EXPECT_GT(drained.datagrams, 0u) << "nothing at all was delivered";
    EXPECT_LT(drained.datagrams, kDatagrams)
        << "the whole burst fit the socket buffer, so the overload path went untested";

    // Reading must keep working after the overload
    const uint32_t recovery_sequence = kDatagrams + 1;
    fill_datagram(datagram.data(), datagram.size(), 0, recovery_sequence);
    ASSERT_TRUE(sender.send_data(datagram.data(), datagram.size()));
    std::ptrdiff_t size = 0;
    ASSERT_TRUE(wait_for([&] { size = receiver.receive(buffer); return size > 0; }, std::chrono::seconds(5)))
        << "the receiver stopped delivering after an overload";
    const ParsedDatagram recovered = parse_datagram(buffer.data(), static_cast<size_t>(size));
    EXPECT_TRUE(recovered.intact) << "a datagram arrived damaged after an overload";
    EXPECT_EQ(recovered.sequence, recovery_sequence);
    sender.stop();

    EXPECT_EQ(receiver.get_packets_received() - packets_before, drained.datagrams + 1);
    EXPECT_EQ(receiver.get_packets_truncated(), 0u);
    EXPECT_EQ(receiver.get_receive_errors(), 0u);
    EXPECT_EQ(sender.get_send_errors(), 0u);
}

// receive() must not allocate. A caller's loop can run for hours, and an allocator lock or free per
// datagram can turn a spike into drops. receive() runs on the test's thread, so the test reads the
// counter directly.
TEST_F(PolledMulticastReceiverPerformanceTest, ReceiveDoesNotAllocate)
{
    constexpr uint64_t kBurst = 100;
    constexpr size_t kDatagramSize = 512;

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    ASSERT_TRUE(receiver.open());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram reached the receiver";

    std::vector<uint8_t> datagram(kDatagramSize);
    for (uint64_t i = 0; i < kBurst; ++i)
    {
        fill_datagram(datagram.data(), datagram.size(), 0, static_cast<uint32_t>(i + 1));
        ASSERT_TRUE(sender.send_data(datagram.data(), datagram.size()));
    }
    sender.stop();

    std::array<uint8_t, 2048> buffer{};
    uint64_t read = 0;
    uint64_t bytes_read = 0;
    slick::socket::test::set_allocation_counting(true);
    for (;;)
    {
        const std::ptrdiff_t size = receiver.receive(buffer);
        if (size <= 0)
        {
            break;
        }
        ++read;
        bytes_read += static_cast<uint64_t>(size);
    }
    slick::socket::test::set_allocation_counting(false);

    EXPECT_EQ(read, kBurst);
    EXPECT_EQ(bytes_read, kBurst * kDatagramSize);
    EXPECT_EQ(slick::socket::test::allocations_on_this_thread(), 0u)
        << "receive() allocated while reading " << kBurst << " datagrams";
}


// Random bursts, each drained before the next. A caller-driven loop sees a feed this way: a spike lands
// while the loop is busy elsewhere, and every datagram must still be queued when it comes back.
TEST_F(PolledMulticastReceiverPerformanceTest, RandomBurstsAreReceivedIntact)
{
    constexpr uint64_t kBudgetBytes = 32 * 1024;
    constexpr int kBursts = 8;
    const uint32_t seed = burst_seed();
    RecordProperty("seed", std::to_string(seed));
    std::mt19937 generator(seed);
    const BurstPattern pattern;

    MulticastSender sender("Sender", sender_config_);
    ASSERT_TRUE(sender.start());
    SEND_FIRST_OR_SKIP(sender, probe_datagram());

    PolledMulticastReceiver receiver("Receiver", receiver_config_);
    ASSERT_TRUE(receiver.open());
    ASSERT_TRUE(probe_until_heard(sender, receiver)) << "no probe datagram reached the receiver";

    const uint64_t packets_before = receiver.get_packets_received();
    const uint64_t bytes_before = receiver.get_bytes_received();

    std::array<uint8_t, 2048> buffer{};
    std::vector<uint8_t> datagram(kMaxDatagram);
    uint64_t sent = 0;
    uint64_t datagrams = 0;
    uint64_t bytes = 0;
    uint64_t duplicates = 0;
    uint64_t corrupt = 0;
    for (int burst = 0; burst < kBursts; ++burst)
    {
        const uint32_t fits = static_cast<uint32_t>(std::max<uint64_t>(kBudgetBytes / pattern.min_size, 1));
        const uint32_t count =
            (std::min)(random_between(generator, pattern.min_datagrams, pattern.max_datagrams), fits);
        uint64_t burst_bytes = 0;
        uint32_t sent_in_burst = 0;
        while (sent_in_burst < count)
        {
            const uint64_t remaining = kBudgetBytes - burst_bytes;
            // Stop once the budget can't fit a minimum-size datagram
            if (remaining < pattern.min_size)
            {
                break;
            }
            const uint32_t size = (std::min)(random_between(generator, pattern.min_size, pattern.max_size),
                                             static_cast<uint32_t>(remaining));
            fill_datagram(datagram.data(), size, 0, static_cast<uint32_t>(sent + 1));
            ASSERT_TRUE(sender.send_data(datagram.data(), size));
            ++sent_in_burst;
            ++sent;
            burst_bytes += size;
            bytes += size;
        }

        const DrainResult drained = drain(receiver, buffer);
        datagrams += drained.datagrams;
        duplicates += drained.duplicates;
        corrupt += drained.corrupt;
        EXPECT_GT(sent_in_burst, 0u) << "burst " << burst << ", seed " << seed;
        EXPECT_EQ(drained.datagrams, sent_in_burst) << "burst " << burst << " of " << kBursts << ", seed " << seed;
    }
    sender.stop();

    std::cout << "[ perf ] " << kBursts << " random bursts, " << datagrams << " of " << sent
              << " datagrams received, " << bytes << " B, seed " << seed << std::endl;

    EXPECT_EQ(corrupt, 0u) << "a datagram arrived cut short or corrupted, seed " << seed;
    EXPECT_EQ(duplicates, 0u);
    EXPECT_EQ(datagrams, sent);
    EXPECT_EQ(receiver.get_packets_received() - packets_before, datagrams);
    EXPECT_EQ(receiver.get_bytes_received() - bytes_before, bytes);
    EXPECT_EQ(receiver.get_packets_truncated(), 0u);
    EXPECT_EQ(receiver.get_receive_errors(), 0u);
    EXPECT_EQ(sender.get_send_errors(), 0u);
}

} // namespace


