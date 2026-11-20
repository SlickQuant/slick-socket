// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 Slick Quant
// https://github.com/SlickQuant/slick-socket

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <slick/socket/logger.h>
#include <slick/socket/polled_socket.h>
#include <slick/socket/worker_thread.h>
#include <vector>
#include <string>
#include <chrono>
#include <atomic>
#include <thread>

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace slick::socket
{

struct MulticastReceiverConfig
{
    std::string multicast_address = "224.0.0.1"; // Multicast group to join
    uint16_t port = 5000;
    std::string interface_address = "0.0.0.0"; // Interface to receive on (0.0.0.0 = any)
    bool reuse_address = true; // Allow multiple receivers on same port
    // On Unix the receiver binds its group, which keeps out unicast and broadcast sent to the port.
    // Set false to bind 0.0.0.0 and get them too. On Windows it always binds 0.0.0.0, since Winsock
    // can't bind a multicast address.
    bool bind_to_group = true;
    int receive_buffer_size = 65536; // SO_RCVBUF, the kernel's queue between reads. Also caps the datagram buffer
    std::chrono::milliseconds receive_timeout{1000}; // Timeout for receive operations
};

template<typename DerivedT>
class MulticastReceiverBase
{
public:
    explicit MulticastReceiverBase(std::string name, const MulticastReceiverConfig& config = MulticastReceiverConfig());
    virtual ~MulticastReceiverBase();

    // Not copyable or movable, because the object owns sockets and its worker thread holds `this`
    MulticastReceiverBase(const MulticastReceiverBase&) = delete;
    MulticastReceiverBase& operator=(const MulticastReceiverBase&) = delete;
    MulticastReceiverBase(MulticastReceiverBase&&) = delete;
    MulticastReceiverBase& operator=(MulticastReceiverBase&&) = delete;

    // Receiver control
    // Call start() and stop() from one thread at a time. They replace the socket and the worker thread
    // without synchronizing against each other. The one supported overlap is stop() from
    // handle_multicast_data() while another thread calls stop(), because on the worker thread stop()
    // only flags the shutdown.
    bool start();
    void stop();

    bool is_running() const noexcept
    {
        return running_.load(std::memory_order_relaxed);
    }

    // Statistics
    uint64_t get_packets_received() const noexcept
    {
        return packets_received_.load(std::memory_order_relaxed);
    }

    uint64_t get_bytes_received() const noexcept
    {
        return bytes_received_.load(std::memory_order_relaxed);
    }

    uint64_t get_receive_errors() const noexcept
    {
        return receive_errors_.load(std::memory_order_relaxed);
    }

protected:
    DerivedT& derived() { return static_cast<DerivedT&>(*this); }
    const DerivedT& derived() const { return static_cast<const DerivedT&>(*this); }

    // Joins the receiver thread, unless called from it (from handle_multicast_data()). Then it returns
    // false, and the loop releases the socket itself after the callback returns.
    bool join_receiver_thread()
    {
        return detail::join_unless_current(receiver_thread_, receiver_thread_id_);
    }

    // receiver_loop() runs on the worker thread and passes each datagram to
    // derived().handle_multicast_data(), which a derived class declares as either
    //
    //   void handle_multicast_data(const uint8_t* data, size_t size, const std::string& sender_address)
    //   void handle_multicast_data(const std::vector<uint8_t>& data, const std::string& sender_address)
    //
    // If the class declares the pointer form, the receiver picks it at compile time and passes its own
    // receive buffer, so nothing is copied or allocated per datagram. The vector form gets a vector
    // sized to the datagram, which costs one copy. Either form must be public.
    void receiver_loop();
    void handle_multicast_data(const std::vector<uint8_t>& data, const std::string& sender_address);

    // Hands one datagram to the derived class in whichever form it declares
    void dispatch_datagram(const uint8_t* data, size_t size, const std::string& sender_address);

#if defined(_WIN32) || defined(_WIN64)
    using SocketT = SOCKET;
    static constexpr SocketT invalid_socket = INVALID_SOCKET;
#else
    using SocketT = int;
    static constexpr SocketT invalid_socket = -1;
#endif

    std::string name_;
    MulticastReceiverConfig config_;
    std::atomic_bool running_{false};

    // Upper bound on a UDP datagram. The length field is 16 bits, and over IPv4 the real limit is a bit
    // lower (65507). A bigger read buffer gains nothing, and a huge length makes every recvfrom() slower
    // (Windows probes the whole range). So receive_buffer_size sizes the kernel's queue and this caps
    // the datagram buffer.
    static constexpr size_t max_datagram_size = 65535;

    SocketT socket_ = invalid_socket;
    std::thread receiver_thread_;
    detail::WorkerThreadId receiver_thread_id_;
    
    // Statistics
    std::atomic<uint64_t> packets_received_{0};
    std::atomic<uint64_t> bytes_received_{0};
    std::atomic<uint64_t> receive_errors_{0};

    // Only the vector form of handle_multicast_data() uses this. assign() copies just the datagram into
    // capacity the vector already has. Resizing it between datagrams would zero-fill the tail each time.
    std::vector<uint8_t> callback_buffer_;

private:
    bool initialize_socket();
    void cleanup_socket();
    bool setup_multicast_options();
    bool join_multicast_group();
    void leave_multicast_group();

    // Leaves the group and closes the socket (no-op when already released)
    void release_socket();

    // Group and interface, resolved once by start() for the bind, join and leave
    ip_mreq membership_{};
};

template<typename DerivedT>
void MulticastReceiverBase<DerivedT>::stop()
{
    const bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (was_running)
    {
        LOG_INFO("Stopping {}...", name_);
    }

    // Wait for receiver thread to finish
    if (!join_receiver_thread())
    {
        // Called from handle_multicast_data(). The loop exits after it returns and cleans up.
        return;
    }

    release_socket();

    if (was_running)
    {
        LOG_INFO("{} stopped", name_);
    }
}

template<typename DerivedT>
void MulticastReceiverBase<DerivedT>::dispatch_datagram(const uint8_t* data, size_t size,
                                                        const std::string& sender_address)
{
    if constexpr (requires(DerivedT& derived, const uint8_t* bytes, size_t count, const std::string& sender) {
                      derived.handle_multicast_data(bytes, count, sender);
                  })
    {
        derived().handle_multicast_data(data, size, sender_address);
    }
    else
    {
        callback_buffer_.assign(data, data + size);
        derived().handle_multicast_data(callback_buffer_, sender_address);
    }
}

} // namespace slick::socket

#if defined(_WIN32) || defined(_WIN64)
#include "multicast_receiver_win32.h"
#else
#include "multicast_receiver_unix.h"
#endif