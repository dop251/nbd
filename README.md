# Go library for Linux NBD support.

It includes an implementation of a server connection handler and a client side connector.

Note that negotiation is out of the scope, the connection needs to be fully configured by the time
the handler starts.

It is currently used for a [BUSE](https://github.com/dop251/buse), but can be also used as a foundation for a
full-fledged server and client implementation.

## Usage

### Server

```go
package main

import (
    "net"
    "os"

    "github.com/dop251/nbd"
)

func SimpleServer(listener net.Listener, filepath string) error {
    f, err := os.OpenFile(filepath, os.O_RDWR, 0600)
    if err != nil {
        return err
    }

    // We want to use a shared process pool for all connections. This ensures no
    // more than 4 requests are being served at any given time.
    pool := nbd.NewProcPool(4)

    for {
        conn, err := listener.Accept()
        if err != nil {
            return err
        }
        go func() {
            dev := nbd.NewServerConn(conn, f)
            dev.SetPool(pool)
            dev.Serve()
        }()
    }

}
```

### Client

```go
package main

import (
    "github.com/dop251/nbd"
)


client := nbd.NewClient("/dev/nbd0", fd, devsize)
client.SetSendFlush(true)
client.SetSendTrim(true)

client.Run()
```
