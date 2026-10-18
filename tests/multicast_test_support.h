#pragma once

#include <gtest/gtest.h>
#include <slick/socket/multicast_sender.h>
#include <slick/socket/polled_socket.h>
#include <cerrno>
#include <cstring>
#include <string>

namespace slick::socket::test
{

// What one send_data() call did, and the OS error it left when it failed
struct SendOutcome
{
    bool sent = false;
    int error = 0;  // errno / WSAGetLastError(); 0 when send_data() failed before reaching sendto()

    // macOS 15+ refuses multicast from a process without the Local Network permission, and a CI runner
    // has no one to grant it. A BSD socket only sees EHOSTUNREACH for that, and nothing more specific
    // exists. Still, a multicast send looped back to this host has no other reason to get it.
    bool local_network_denied() const noexcept
    {
#if defined(__APPLE__)
        return !sent && error == EHOSTUNREACH;
#else
        return false;
#endif
    }

    std::string describe() const
    {
        if (sent)
        {
            return "sent";
        }
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
};

// Sends `message` and records the OS error a failed sendto() left, which send_data() does not overwrite
// on its way out. The error is cleared first, so a send failing before sendto() is not blamed on an
// earlier call.
inline SendOutcome try_send(MulticastSender& sender, const std::string& message)
{
#if defined(_WIN32) || defined(_WIN64)
    WSASetLastError(0);
#else
    errno = 0;
#endif
    if (sender.send_data(message))
    {
        return {true, 0};
    }
    return {false, detail::last_socket_error()};
}

// Whether this host denies multicast sends to `config`'s group for lack of the Local Network permission.
// Probes with a sender of its own, so the caller's statistics are untouched.
inline bool local_network_denied(const MulticastSenderConfig& config)
{
    MulticastSender probe("LocalNetworkProbe", config);
    return probe.start() && try_send(probe, "probe").local_network_denied();
}

} // namespace slick::socket::test

// A test's first multicast send. Where macOS denies the Local Network permission there's nothing to
// test, so the test is skipped. Any other failure fails it. Once one send has worked, later ones must too.
#define SEND_FIRST_OR_SKIP(sender, message)                                                            \
    do                                                                                                 \
    {                                                                                                  \
        const ::slick::socket::test::SendOutcome send_outcome_ =                                       \
            ::slick::socket::test::try_send((sender), (message));                                      \
        if (send_outcome_.local_network_denied())                                                      \
        {                                                                                              \
            GTEST_SKIP() << "macOS denied the Local Network permission: " << send_outcome_.describe(); \
        }                                                                                              \
        ASSERT_TRUE(send_outcome_.sent) << "Multicast send failed: " << send_outcome_.describe();      \
    } while (0)
