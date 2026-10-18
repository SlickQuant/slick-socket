# slick-socket

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/SlickQuant/slick-socket/actions/workflows/ci.yml/badge.svg)](https://github.com/SlickQuant/slick-socket/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/SlickQuant/slick-socket)](https://github.com/SlickQuant/slick-socket/releases)

slick-socket is a header-only C++20 library for TCP and UDP multicast networking on Windows, Linux and macOS.

## Features

- Header-only. Most components use CRTP, so there are no virtual calls.
- TCP server and client on non-blocking sockets, with timeouts.
- UDP multicast sender and receiver.
- Caller-driven TCP client and multicast receiver with no thread of their own. Your loop reads from them directly, without handing data between threads or allocating per packet.
- Logging through the `LOG_DEBUG`, `LOG_INFO`, `LOG_WARN`, `LOG_ERROR` and `LOG_TRACE` macros. Define them before including the headers to plug in your own logger. By default they do nothing.

## Dependencies

On Windows, slick-socket needs [wepoll](https://github.com/piscisaureus/wepoll), which provides an epoll-style API. Install it with `vcpkg install wepoll`, or let CMake download and build it when it can't find one. Linux and macOS need nothing else.

## Installation

### Using vcpkg (recommended on Windows)

Install wepoll first:

```bash
vcpkg install wepoll
```

Then configure with the vcpkg toolchain:

```bash
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=[path-to-vcpkg]/scripts/buildsystems/vcpkg.cmake
```

CMake finds both of vcpkg's wepoll builds, Release (`lib/wepoll.lib`) and Debug (`debug/lib/wepoll.lib`), and links each build configuration against the matching one, so Debug and Release never mix C runtimes. This works both when building slick-socket and in projects that use `find_package(slick-socket)`. To use a different wepoll, set `WEPOLL_INCLUDE_DIR` plus either `WEPOLL_LIBRARY_RELEASE` and `WEPOLL_LIBRARY_DEBUG`, or a single `WEPOLL_LIBRARY` for every configuration.

### Using FetchContent

The simplest option is to fetch slick-socket from your CMakeLists.txt:

```cmake
include(FetchContent)

# Disable slick-socket examples and tests
set(BUILD_SLICK_SOCKET_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_SLICK_SOCKET_TESTING OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    slick-socket
    GIT_REPOSITORY https://github.com/SlickQuant/slick-socket.git
    GIT_TAG v1.3.0  # Use the desired version
)

FetchContent_MakeAvailable(slick-socket)

# Link against slick-socket (automatically links ws2_32 and wepoll on Windows)
target_link_libraries(your_target PRIVATE slick::socket)
```

### Using find_package

If slick-socket is installed:

```cmake
find_package(slick-socket REQUIRED)
target_link_libraries(your_target PRIVATE slick::socket)
```

However you get it (installed, `FetchContent` or `add_subdirectory`), linking `slick::socket` builds `your_target` as C++20 or later, so you don't need to set `CMAKE_CXX_STANDARD`.

### Using a release archive

Each [GitHub release](https://github.com/SlickQuant/slick-socket/releases) has Linux, macOS and Windows archives of the `cmake --install` output, which holds the headers and the CMake package files. The Windows archive also contains `wepoll.h` and Debug (`wepoll_libd.lib`) and Release (`wepoll_lib.lib`) builds of wepoll, so there's nothing else to install. Extract an archive anywhere, point CMake at it, and use the `find_package` snippet above:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/extracted/slick-socket
```

The Windows wepoll libraries are built with MSVC against the dynamic C runtime (`/MD`, `/MDd`).

### From source

#### Prerequisites

- A C++20 compiler: GCC 11+, Clang 12+ or MSVC 2022+
- CMake 3.25 or later
- On Windows, wepoll (downloaded automatically if not found)

#### Unix/Linux/macOS

1. Configure the build:
   ```bash
   cmake -S . -B build
   ```

2. Build the library:
   ```bash
   cmake --build build --config Release
   ```

3. Install (optional):
   ```bash
   cmake --install build --prefix /path/to/install
   ```

   The library is header-only and needs nothing else on these platforms, so you can also just copy `include/slick` into your project.

#### Windows (Visual Studio)

1. Install wepoll with vcpkg (optional):
   ```bash
   vcpkg install wepoll
   ```

2. Configure the build:
   ```bash
   # With vcpkg
   cmake -S . -B build -G "Visual Studio 17 2022" -DCMAKE_TOOLCHAIN_FILE=[path-to-vcpkg]/scripts/buildsystems/vcpkg.cmake

   # Without vcpkg (wepoll will be fetched automatically)
   cmake -S . -B build -G "Visual Studio 17 2022"
   ```

3. Build the library:
   ```bash
   cmake --build build --config Release
   ```

4. Install (optional):
   ```bash
   cmake --install build --prefix /path/to/install
   ```

   Then in your project:
   ```cmake
   find_package(slick-socket REQUIRED)
   target_link_libraries(your_target PRIVATE slick::socket)
   ```

## Usage

### Headers

Each component has its own header:

```cpp
#include <slick/socket/tcp_server.h>
#include <slick/socket/tcp_client.h>
#include <slick/socket/multicast_sender.h>
#include <slick/socket/multicast_receiver.h>
```

### Creating a TCP server

```cpp
#include <slick/socket/tcp_server.h>

class MyServer : public slick::socket::TCPServerBase<MyServer>
{
public:
    MyServer() : TCPServerBase("MyServer", {/*.port = 5000*/}) {}

    void onClientConnected(int client_id, const std::string& address)
    {
        std::cout << "Client " << client_id << " connected from " << address << std::endl;
    }

    void onClientData(int client_id, const uint8_t* data, size_t size)
    {
        // Echo back to client
        send_data(client_id, std::vector<uint8_t>(data, data + size));
    }

    void onClientDisconnected(int client_id)
    {
        std::cout << "Client " << client_id << " disconnected" << std::endl;
    }
};

int main()
{
    MyServer server;
    server.start();
    // ... server runs in background thread
    server.stop();
    return 0;
}
```

Things to know about the server:

- `TCPServerConfig::port = 0` lets the OS pick a free port. After `start()`, `port()` returns the port it bound.
- `TCPServerConfig::max_connections` limits how many clients can be connected at once. A connection over the limit is accepted and closed straight away, so that client sees a disconnect. `0` or less means no limit.
- You can call `stop()` from a server callback. No more callbacks run, and the server thread closes its sockets once the callback returns. Call `start()` again to restart the server.
- `TCPServerConfig::idle_timeout` disconnects a client after that long with no traffic in either direction (nothing received and no send progress), and reports it through `onClientDisconnected()`. A client that only receives stays connected as long as the server keeps sending to it and it keeps reading. An idle client is closed within 1/8 of the timeout after it expires. The default, `0`, turns this off.
- `onClientDisconnected()` runs exactly once for every client that leaves, whether the peer closed, an I/O error happened, or the server called `disconnect_client()`. A disconnect caused inside a callback, including by a failed `send_data()`, is reported after that callback returns, never from inside it. Clients still connected when the server stops are closed without this callback.
- slick-socket never changes the process's signal handling. On Unix, writing to a peer that has closed the connection fails the send and disconnects instead of raising `SIGPIPE`. It does this with `MSG_NOSIGNAL` and, where available (macOS, BSD), `SO_NOSIGPIPE` on each TCP socket. The same goes for `TCPClientBase`.
- `send_data()` never blocks the server thread. Whatever a slow client can't take yet is queued for that client and sent when its socket becomes writable. If a message would push the queue past `TCPServerConfig::max_pending_send_bytes` (default 16 MiB, `0` for no limit), `send_data()` returns `false` and drops the whole message before writing any of it, so the stream stays intact. A single message bigger than the limit is therefore always rejected. Call `send_data()` and `disconnect_client()` on the server thread, which means from a server callback.
- `send_data(client_id, const uint8_t* data, size_t size)` sends straight from your buffer. The `std::vector` and `std::string` overloads forward to it, so none of them copy.
- If the derived class declares a public `void onPoll()`, the server thread calls it once per loop iteration, whether or not anything happened. Use it for traffic that doesn't start with a client, such as draining a queue another thread fills, or running timers. It can call `send_data()` and `disconnect_client()`. The server detects it at compile time, so a server without it pays nothing. With `cpu_affinity` set, iterations run back to back; otherwise they're at most about 1 ms apart.
- With `cpu_affinity` set, the server thread busy-polls for the lowest latency. Otherwise it blocks in the event loop.

### Creating a TCP client

```cpp
#include <slick/socket/tcp_client.h>

class MyClient : public slick::socket::TCPClientBase<MyClient>
{
public:
    MyClient(const slick::socket::TCPClientConfig& config)
        : TCPClientBase("MyClient", config) {}

    void onConnected()
    {
        std::cout << "Connected to server" << std::endl;
    }

    void onDisconnected()
    {
        std::cout << "Disconnected from server" << std::endl;
    }

    void onData(const uint8_t* data, size_t length)
    {
        std::string received_data((const char*)data, length);
        std::cout << "Received: " << received_data << std::endl;
    }
};

int main()
{
    slick::socket::TCPClientConfig config;
    config.server_address = "127.0.0.1";   // IPv4 address or hostname (default: "localhost")
    config.server_port = 5000;

    MyClient client(config);
    client.connect();

    if (client.is_connected())
    {
        client.send_data("Hello Server!");
        // ... process responses
        client.disconnect();
    }

    return 0;
}
```

`onConnected()` runs on the thread that called `connect()`, before the client thread starts. So `onData()` and `onDisconnected()` can't run until it returns, and anything the server sends in the meantime waits in the socket. If `onConnected()` calls `disconnect()`, `onDisconnected()` is reported right away and `connect()` returns `false`.

When the server closes the connection, or `send_data()` finds it broken, `onDisconnected()` is called and `is_connected()` turns false. This holds even right after connecting, in which case `connect()` still returns `true` and `onDisconnected()` reports the loss. You can call `connect()` again to reconnect the same object, but not from the client's own callbacks. There `connect()` returns `false`, because it would first have to join the finished client thread, which is the thread it's running on. To reconnect after `onDisconnected()`, have the callback tell another thread to call `connect()`.

While idle, the client thread blocks in `poll()`. Setting `TCPClientConfig::cpu_affinity` pins it to a core and switches it to busy-polling `recv()` for the lowest latency. `send_data()` blocks the calling thread, without spinning, until the server has accepted all of the data.

Any number of threads can call `send_data()`, even while another thread calls `disconnect()`. The socket isn't closed until every send in progress has finished, so a send never uses a closed or reused socket. Sends take no lock; each costs two atomic operations. When a large message goes out in parts, concurrent `send_data()` calls can interleave their bytes, so serialize them if message boundaries matter. `connect()` and `disconnect()` must not run at the same time as each other. Threads that keep calling `send_data()` never hold up a thread that is stopping or disconnecting, because a rejected send doesn't count as in progress.

> **Lifetime:** stop servers, clients and multicast receivers (`stop()` or `disconnect()`, called from outside their worker thread) before the derived object is destroyed, for example in the derived destructor. The base destructor runs after the derived members are gone, so a callback still running at that point would touch destroyed state. Make the call even if the connection has already ended, for example because the server closed a client's connection, the client's `send_data()` found it broken, or a callback called `stop()`. That outside call is what joins the finished worker thread.
>
> If you destroy a running object, slick-socket logs an error and calls `SLICK_SOCKET_ON_UNSAFE_DESTROY()`, which asserts in debug builds. To handle it differently, for example to count it or abort, define that macro before including any slick-socket header. It runs inside a destructor, so it must not throw; an exception escaping it calls `std::terminate()`. A client that is destroyed while still connected skips `onDisconnected()`.

### Creating a multicast sender

```cpp
#include <slick/socket/multicast_sender.h>

int main()
{
    slick::socket::MulticastSenderConfig config;
    config.multicast_address = "224.0.0.100";
    config.port = 12345;
    config.ttl = 1; // Local network only

    slick::socket::MulticastSender sender("MySender", config);

    if (!sender.start())
    {
        std::cerr << "Failed to start sender" << std::endl;
        return -1;
    }

    // Send data to multicast group
    sender.send_data("Hello Multicast World!");

    // Check statistics
    std::cout << "Packets sent: " << sender.get_packets_sent() << std::endl;

    sender.stop();
    return 0;
}
```

Any number of threads can call `send_data()`, even while another thread calls `stop()`. The socket isn't closed until every send in progress has finished. Sends take no lock, and each call sends one datagram. `send_data(const uint8_t* data, size_t size)` sends straight from your buffer, so a publisher that builds packets in place doesn't copy or allocate per datagram. The group address is resolved once, in `start()`. An invalid address is logged there, and every later send fails and counts as a send error. When the send buffer is full, `send_data()` waits for room. Once `stop()` is called it returns `false` within about 1 ms, so a stalled network can't hold up `stop()`.

On macOS 15 and later, a process needs the Local Network permission to send multicast. Without it, every `sendto()` fails with `EHOSTUNREACH` ("No route to host") and `send_data()` returns `false`. macOS asks the user the first time a program sends, so a process with nobody to answer the prompt, such as one on a CI runner, is simply refused. See Apple's [TN3179: Understanding local network privacy](https://developer.apple.com/documentation/technotes/tn3179-understanding-local-network-privacy).

### Creating a multicast receiver

```cpp
#include <slick/socket/multicast_receiver.h>

class MyReceiver : public slick::socket::MulticastReceiverBase<MyReceiver>
{
public:
    MyReceiver(const slick::socket::MulticastReceiverConfig& config)
        : MulticastReceiverBase("MyReceiver", config) {}

    void handle_multicast_data(const std::vector<uint8_t>& data, const std::string& sender_address)
    {
        std::string message(data.begin(), data.end());
        std::cout << "Received from " << sender_address << ": " << message << std::endl;
    }
};

int main()
{
    slick::socket::MulticastReceiverConfig config;
    config.multicast_address = "224.0.0.100";
    config.port = 12345;
    config.reuse_address = true; // Allow multiple receivers

    MyReceiver receiver(config);

    if (!receiver.start())
    {
        std::cerr << "Failed to start receiver" << std::endl;
        return -1;
    }

    // ... receiver runs in background thread
    std::this_thread::sleep_for(std::chrono::seconds(30));

    receiver.stop();
    return 0;
}
```

You can call `stop()` from `handle_multicast_data()`. The receiver thread leaves the group and closes its socket once the callback returns.

### Caller-driven sockets

`TCPClientBase` and `MulticastReceiverBase` each run their own thread and hand you data in a callback. `PolledTCPClient` and `PolledMulticastReceiver` have no thread. Your loop asks them for data, and a call that finds nothing returns immediately. A busy-polling thread reads the bytes itself, so there's no wake-up, no hand-off between threads and no allocation per packet. One thread can serve any number of sockets in whatever order it likes.

```cpp
#include <slick/socket/polled_tcp_client.h>
#include <slick/socket/polled_multicast_receiver.h>
#include <array>

int main()
{
    slick::socket::PolledTCPClientConfig tcp;
    tcp.server_address = "127.0.0.1";
    tcp.server_port = 5000;
    slick::socket::PolledTCPClient client("OrderEntry", tcp);
    client.connect();                       // returns at once; poll() completes it

    slick::socket::PolledMulticastReceiverConfig group;
    group.multicast_address = "224.0.0.1";
    group.port = 5001;
    group.receive_buffer_size = 4 << 20;    // what the kernel holds between two receive() calls
    slick::socket::PolledMulticastReceiver feed("Feed", group);
    feed.open();

    std::array<uint8_t, 1500> packet;
    std::array<uint8_t, 65536> stream;
    for (;;)
    {
        if (const auto size = feed.receive(packet); size > 0)
        {
            // one datagram, in packet[0 .. size)
        }

        using State = slick::socket::PolledTCPClient::State;
        if (client.state() == State::Connecting)
        {
            client.poll();                  // made, failed or timed out
        }
        else if (const auto size = client.receive(stream); size > 0)
        {
            // stream[0 .. size) - the next bytes of the TCP stream
        }
        else if (size < 0)
        {
            client.connect();               // the connection is gone: start another
        }
    }
}
```

| Call | Returns |
| --- | --- |
| `PolledTCPClient::connect()` | `false` if the attempt couldn't be started. It never waits for the network, but resolving a hostname here can block; an IPv4 literal doesn't. |
| `PolledTCPClient::poll()` | The current state: `Connecting` until the connection is made (`Connected`), fails, or runs past `connection_timeout` (`Disconnected`). |
| `PolledTCPClient::receive(buffer)` | The number of bytes read, `0` if nothing is waiting, or `-1` if the connection is gone. |
| `PolledTCPClient::send(data)` | `false` if not all of it could be sent, in which case the connection is closed. Waits up to `send_timeout` for a server that has stopped reading. |
| `PolledMulticastReceiver::receive(buffer)` | The datagram's size, `0` if none is waiting, or `-1` on an error. A datagram longer than the buffer is cut to fit and counted by `get_packets_truncated()`. |

`PolledTCPClient` sets `TCP_NODELAY` by default, so each `send()` goes out immediately. Each object belongs to one thread; nothing in either class is synchronized.

### Threading

| Calls | Thread rule |
| --- | --- |
| Control: `start()`/`stop()` (server, multicast sender and receiver), `connect()`/`disconnect()` (client) | One thread at a time per object. These calls replace the socket and the worker thread and aren't synchronized with each other, so overlapping them (say, `stop()` on one thread while `start()` runs on another) is undefined. The one supported overlap is calling `stop()`/`disconnect()` from the object's own callback while another thread calls `stop()`/`disconnect()`; on the worker thread, the call only flags the shutdown. |
| `send_data()` on `TCPClientBase` and `MulticastSender` | Any thread, at the same time as each other and as the control calls. |
| `send_data()`/`disconnect_client()` on `TCPServerBase` | Only the server thread, which means from a server callback, `onPoll()` included. |
| `is_running()`, `is_connected()`, `get_connected_client_count()`, statistics getters | Any thread. |
| Everything on `PolledTCPClient` and `PolledMulticastReceiver` | Only the thread that owns the object. |

For more examples, see the [examples/](examples/) directory.

## Testing

Run the whole test suite:

```bash
cd build && ctest --output-on-failure -C Debug
```

Run specific tests:

```bash
cd build && ctest -R TCPServerTest -C Debug
```

Show verbose output:

```bash
cd build && ctest -V -C Debug
```

Some tests skip where the behavior they check doesn't exist or can't be measured, such as `SIGPIPE` on Windows. The multicast tests also skip when macOS refuses to send because the process lacks the Local Network permission, as on GitHub's macOS runners. The skip message gives the OS error, `error 65 (No route to host)`, and any other multicast send failure fails the test. `--output-on-failure` doesn't show why a test was skipped. Look in `build/Testing/Temporary/LastTest.log` for the reason; the CI workflow prints it after the tests.

## Development

### Build options

#### AddressSanitizer

```bash
cmake -S . -B build -DENABLE_ASAN=ON
cmake --build build --config Debug
```

#### Optimized release build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

#### Compiler warnings

Tests and examples build with `/W4` (MSVC) or `-Wall -Wextra -Wpedantic` (GCC, Clang), which keeps the headers warning-free for projects that compile at high warning levels. `SLICK_SOCKET_WARNINGS_AS_ERRORS` adds `/WX` or `-Werror`. It defaults to `ON` only when slick-socket is the top-level project, so pulling it in with `FetchContent` or `add_subdirectory` won't break your build when a newer compiler adds warnings. These flags never apply to the `slick::socket` target itself. To turn the option off:

```bash
cmake -S . -B build -DSLICK_SOCKET_WARNINGS_AS_ERRORS=OFF
```

### Project structure

```
slick-socket/
├── include/slick/socket/     # Public headers (header-only)
│   ├── tcp_server.h          # TCP server base class
│   ├── tcp_client.h          # TCP client base class
│   ├── multicast_sender.h    # UDP multicast sender
│   ├── multicast_receiver.h  # UDP multicast receiver
│   ├── polled_tcp_client.h   # Caller-driven TCP client (no thread, no callbacks)
│   ├── polled_multicast_receiver.h # Caller-driven UDP multicast receiver
│   ├── polled_socket.h       # What the caller-driven sockets share across platforms (internal)
│   ├── *_win32.h / *_unix.h  # Platform implementations, included by the headers above
│   ├── worker_thread.h       # Worker-thread identity helper (internal)
│   ├── send_gate.h           # Lock-free guard keeping a socket open during send_data() (internal)
│   ├── socket_options_unix.h # SIGPIPE suppression per socket (internal)
│   └── logger.h              # Logger interface
├── cmake/                     # CMake package config template
├── examples/                  # Usage examples
├── tests/                     # Unit and integration tests
└── CMakeLists.txt
```

## Architecture

The TCP server, TCP client, multicast sender and multicast receiver are each split into three files so they build on every platform:

- `component.h`: the base class, with the platform-independent interface
- `component_win32.h`: the Windows implementation
- `component_unix.h`: the Linux and macOS implementation

Most components use CRTP (the Curiously Recurring Template Pattern) for compile-time polymorphism, so there's no virtual-call overhead. `MulticastSender` is a plain class without CRTP, which keeps it simple to use. The caller-driven `PolledTCPClient` and `PolledMulticastReceiver`, which make no callbacks, are plain classes too. Each of those two is a single header, and `polled_socket.h` holds their few platform differences.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

**Made with ⚡ by [SlickQuant](https://github.com/SlickQuant)**
