# slick-socket

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/SlickQuant/slick-socket/actions/workflows/ci.yml/badge.svg)](https://github.com/SlickQuant/slick-socket/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/SlickQuant/slick-socket)](https://github.com/SlickQuant/slick-socket/releases)

A header-only C++20 networking library providing cross-platform TCP and UDP multicast communication.

## Features

- **Cross-platform**: Windows and Unix/Linux support
- **Header-only**: No separate compilation required
- **Modern C++**: C++20 design with CRTP for most components
- **Asynchronous**: Non-blocking socket operations with timeout handling
- **TCP Communication**: Client and server implementations
- **UDP Multicast**: One-to-many communication support
- **Logging**: Pluggable via `LOG_DEBUG`/`LOG_INFO`/`LOG_WARN`/`LOG_ERROR`/`LOG_TRACE` macros defined before including the headers; no-op by default

## Dependencies

- **Windows**: Requires [wepoll](https://github.com/piscisaureus/wepoll) for epoll-like functionality
  - Automatically fetched via CMake FetchContent if not installed
  - Or install via vcpkg: `vcpkg install wepoll`
- **Unix/Linux/macOS**: No external dependencies

## Installation

### Using vcpkg (Recommended for Windows users)

If you're using vcpkg, install wepoll first:

```bash
vcpkg install wepoll
```

Then use CMake with the vcpkg toolchain:

```bash
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=[path-to-vcpkg]/scripts/buildsystems/vcpkg.cmake
```

### Using FetchContent

The easiest way to use slick-socket is to fetch it directly in your CMakeLists.txt:

```cmake
include(FetchContent)

# Disable slick-socket examples and tests
set(BUILD_SLICK_SOCKET_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_SLICK_SOCKET_TESTING OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    slick-socket
    GIT_REPOSITORY https://github.com/SlickQuant/slick-socket.git
    GIT_TAG v1.0.6  # Use the desired version
)

FetchContent_MakeAvailable(slick-socket)

# Link against slick-socket (automatically links ws2_32 and wepoll on Windows)
target_link_libraries(your_target PRIVATE slick::socket)
```

**Note**: On Windows, if wepoll is not found, CMake will automatically fetch and build it from GitHub.

### Using find_package

If you have slick-socket installed, you can use it with `find_package`:

```cmake
find_package(slick-socket REQUIRED)
target_link_libraries(your_target PRIVATE slick::socket)
```

Linking `slick::socket` (installed, `FetchContent`, or `add_subdirectory`) compiles `your_target` as C++20 or later; there is no need to set `CMAKE_CXX_STANDARD` yourself.

### Using a Release Archive

Each [GitHub release](https://github.com/SlickQuant/slick-socket/releases) has a Linux, macOS and Windows archive containing the `cmake --install` output: the headers and the CMake package files. The Windows archive also includes `wepoll.h` plus Debug (`wepoll_libd.lib`) and Release (`wepoll_lib.lib`) builds of wepoll, so nothing else needs to be installed. Extract it anywhere and point CMake at it, then use the `find_package` snippet above:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/extracted/slick-socket
```

The Windows wepoll libraries are built with MSVC and the dynamic C runtime (`/MD`, `/MDd`).

### From Source

#### Prerequisites

- C++20 compatible compiler (GCC 11+, Clang 12+, MSVC 2022+)
- CMake 3.25 or higher
- **Windows only**: wepoll (automatically fetched if not found)

#### Unix/Linux/macOS

1. **Configure the build**:
   ```bash
   cmake -S . -B build
   ```

2. **Build the library**:
   ```bash
   cmake --build build --config Release
   ```

3. **Copy to your project**:
   ```bash
   cp -r build/dist/include/slick /path/to/your/project/include/
   ```

   The library is header-only on Unix/Linux/macOS platforms, so only headers are needed.

#### Windows (Visual Studio)

1. **(Optional) Install wepoll via vcpkg**:
   ```bash
   vcpkg install wepoll
   ```

2. **Configure the build**:
   ```bash
   # With vcpkg
   cmake -S . -B build -G "Visual Studio 17 2022" -DCMAKE_TOOLCHAIN_FILE=[path-to-vcpkg]/scripts/buildsystems/vcpkg.cmake

   # Without vcpkg (wepoll will be fetched automatically)
   cmake -S . -B build -G "Visual Studio 17 2022"
   ```

3. **Build the library**:
   ```bash
   cmake --build build --config Release
   ```

4. **Install (optional)**:
   ```bash
   cmake --install build --prefix /path/to/install
   ```

   Then in your project:
   ```cmake
   find_package(slick-socket REQUIRED)
   target_link_libraries(your_target PRIVATE slick::socket)
   ```

## Usage

### Basic Example

Include the headers you need in your project:

```cpp
#include <slick/socket/tcp_server.h>
#include <slick/socket/tcp_client.h>
#include <slick/socket/multicast_sender.h>
#include <slick/socket/multicast_receiver.h>
```

### Creating a TCP Server

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

Server configuration notes:

- Set `TCPServerConfig::port = 0` to let the OS pick a free port; `port()` returns the bound port after `start()`.
- `TCPServerConfig::max_connections` caps concurrent clients. Connections beyond the limit are accepted and closed immediately (the client sees a disconnect). A value `<= 0` means unlimited.
- `stop()` may be called from a server callback: no further callbacks are dispatched, and the server thread closes its sockets once the callback returns. A later `start()` restarts the server.
- `TCPServerConfig::idle_timeout` disconnects a client after that long with no traffic in either direction (nothing received and no send progress), and reports it through `onClientDisconnected()`. A receive-only client stays connected as long as the server keeps sending to it and it keeps reading. Idle clients are closed within 1/8 of the timeout after it expires. `0` (the default) disables it.
- `onClientDisconnected()` fires exactly once for every client that leaves, whether the peer closed, an I/O error occurred, or the server called `disconnect_client()`. A disconnect triggered from a callback, including a failed `send_data()`, is reported after that callback returns, never from inside it. Clients still connected when the server stops are closed without this callback.
- `get_connected_client_count()` is safe to call from any thread.
- slick-socket never changes the process's signal handling. On Unix, writing to a peer that has closed the connection fails the send (and disconnects) instead of raising `SIGPIPE`, using `MSG_NOSIGNAL` and, where available (macOS/BSD), `SO_NOSIGPIPE` on each TCP socket. This applies to `TCPClientBase` too.
- `send_data()` never blocks the server thread. Data a slow client cannot take right away is queued per client and flushed when its socket becomes writable. If queuing a message could exceed `TCPServerConfig::max_pending_send_bytes` (default 16 MiB, `0` = unlimited), `send_data()` returns `false` and drops that whole message before writing any of it, so the stream stays intact. A single message larger than the limit is therefore always rejected. Call `send_data()` / `disconnect_client()` on the server thread, i.e. from a server callback.
- With `cpu_affinity` set, the server thread busy-polls for the lowest latency; otherwise it blocks in the event loop.

### Creating a TCP Client

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

`onConnected()` runs on the thread that calls `connect()`, before the client thread starts, so `onData()` and `onDisconnected()` never run until it has returned; data the server sends meanwhile waits in the socket. Calling `disconnect()` from `onConnected()` reports `onDisconnected()` right away and makes `connect()` return `false`.

If the server closes the connection, or a `send_data()` finds it broken, `onDisconnected()` is called and `is_connected()` becomes false. Calling `connect()` again reconnects the same client object, but not from the client's own callbacks: there `connect()` returns `false`, because it must first join the finished client thread, which is the calling thread. To reconnect after `onDisconnected()`, have the callback signal another thread to call `connect()`.

The client thread blocks in `poll()` while idle; setting `TCPClientConfig::cpu_affinity` pins it to a core and switches to busy-polling `recv()` for the lowest latency. `send_data()` blocks the calling thread (without spinning) until the server has accepted all of the data.

`send_data()` may be called from any number of threads, including while another thread calls `disconnect()`: the socket is not closed until every send in progress has finished, so a send never uses a closed or reused socket. Sends take no lock; each costs two atomic operations. Concurrent `send_data()` calls can interleave their bytes when a large message is written in parts, so serialize them if message boundaries matter. `connect()` and `disconnect()` must not run concurrently with each other. A thread that stops or disconnects is never held up by other threads that keep calling `send_data()`: a rejected send does not count as in progress.

> **Lifetime:** servers, clients and multicast receivers must be stopped (`stop()` / `disconnect()`, called from outside their worker thread) before the derived object is destroyed, e.g. in the derived destructor. This also applies after the server closed a client's connection, after a client's `send_data()` found the connection broken, or after `stop()` was called from a callback: the outside call joins the finished worker thread. The base destructor runs after the derived members are gone, so a callback still running at that point would touch destroyed state. Destroying a running object is reported: an error is logged and `SLICK_SOCKET_ON_UNSAFE_DESTROY()` is invoked, which asserts in debug builds. Define that macro before including any slick-socket header to handle it differently (e.g. count or abort). It runs inside a destructor, so it must not throw: an escaping exception calls `std::terminate()`. As a last-resort safety net, a client destroyed while connected skips `onDisconnected()`.

### Creating a Multicast Sender

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

`send_data()` may be called from any number of threads, including while another thread calls `stop()`: the socket is not closed until every send in progress has finished. Sends take no lock and each call sends one datagram. If the send buffer is full, `send_data()` waits for room, but returns `false` within about 1 ms once `stop()` is called, so a stalled network cannot hold up `stop()`.

### Creating a Multicast Receiver

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

`stop()` may be called from `handle_multicast_data()`; the receiver thread leaves the group and closes its socket once the callback returns.

### Threading

| Calls | Thread rule |
| --- | --- |
| Control: `start()`/`stop()` (server, multicast sender and receiver), `connect()`/`disconnect()` (client) | One thread at a time per object. These replace the socket and worker thread and are not synchronized against each other, so overlapping them (e.g. `stop()` on one thread while `start()` runs on another) is undefined. The one supported overlap is `stop()`/`disconnect()` from the object's own callback while another thread calls `stop()`/`disconnect()`: on the worker thread it only flags the shutdown. |
| `send_data()` on `TCPClientBase` and `MulticastSender` | Any thread, concurrently with each other and with the control calls. |
| `send_data()`/`disconnect_client()` on `TCPServerBase` | The server thread only, i.e. from a server callback. |
| `is_running()`, `is_connected()`, `get_connected_client_count()`, statistics getters | Any thread. |

For more examples, see the [examples/](examples/) directory.

## Testing

Run the complete test suite:

```bash
cd build && ctest --output-on-failure -C Debug
```

Run specific tests:

```bash
cd build && ctest -R TCPServerTest -C Debug
```

Enable verbose test output:

```bash
cd build && ctest -V -C Debug
```

## Development

### Build Options

#### AddressSanitizer

Enable AddressSanitizer for debugging memory issues:

```bash
cmake -S . -B build -DENABLE_ASAN=ON
cmake --build build --config Debug
```

#### Release Build with Optimization

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

#### Compiler Warnings

Tests and examples build with `/W4` (MSVC) or `-Wall -Wextra -Wpedantic` (GCC/Clang), so the headers stay clean for consumers at high warning levels. `SLICK_SOCKET_WARNINGS_AS_ERRORS` adds `/WX` / `-Werror`; it defaults to `ON` only when slick-socket is the top-level project, so pulling it in through `FetchContent` or `add_subdirectory` never fails on a newer compiler's warnings. The flags never apply to the `slick::socket` target itself.

```bash
cmake -S . -B build -DSLICK_SOCKET_WARNINGS_AS_ERRORS=OFF
```

### Project Structure

```
slick-socket/
├── include/slick/socket/     # Public headers (header-only)
│   ├── tcp_server.h          # TCP server base class
│   ├── tcp_client.h          # TCP client base class
│   ├── multicast_sender.h    # UDP multicast sender
│   ├── multicast_receiver.h  # UDP multicast receiver
│   ├── *_win32.h / *_unix.h  # Platform implementations, included by the headers above
│   ├── worker_thread.h       # Worker-thread identity helper (internal)
│   └── logger.h              # Logger interface
├── cmake/                     # CMake package config template
├── examples/                  # Usage examples
├── tests/                     # Unit and integration tests
└── CMakeLists.txt
```

## Architecture

The library uses a three-file pattern for cross-platform support:

- `component.h` - Base class with platform-independent interface
- `component_win32.h` - Windows implementation
- `component_unix.h` - Unix/Linux implementation

Most components use CRTP (Curiously Recurring Template Pattern) for compile-time polymorphism without virtual function overhead. `MulticastSender` is implemented as a regular class without CRTP for simpler usage.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

**Made with ⚡ by [SlickQuant](https://github.com/SlickQuant)**