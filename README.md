# TickerFeed

TickerFeed is a high performance, low latency, real-time market feed websocket API system.

It has two main components:

1. Broker - A UDP server that receives market data from a market data provider and distributes to server nodes.
2. Server - A websocket server that receives market data from the broker and distributes to clients.

## Development

This project is built with Go. To test and build, just run `make`.

```shell
# To build the system binary
$ make build

# To run tests
$ make test
```