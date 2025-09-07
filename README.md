# Gort Redis Streams Client

A Go module providing Redis Streams integration for cryptocurrency liquidation heatmap services. This client handles publishing, consuming, and caching of liquidation data across multiple exchanges.

## Features

- 🚀 **High-performance** Redis Streams publishing and consuming
- 📊 **Multi-exchange support** (Binance, OKX, Bybit, Coinbase, Kraken, Deribit, Bitfinex)
- 🔄 **Consumer groups** for scalable message processing
- 💾 **Built-in caching** for heatmap data
- 📈 **Stream monitoring** and management utilities
- 🧹 **Automatic cleanup** and trimming capabilities
- ✅ **Comprehensive validation** and error handling

## Installation

```bash
go get github.com/bohunn/gort-redis-streams-client
```

## Dependencies

This module requires:
- [gort-trade-model](https://github.com/bohunn/gort-trade-model) - Data structures
- [go-redis/v9](https://github.com/redis/go-redis) - Redis client

## Quick Start

### 1. Setup Redis Streams

Use the included setup script to initialize your Redis streams:

```bash
chmod +x setup.sh
./setup.sh
```

### 2. Create a Client

```go
package main

import (
    "github.com/bohunn/gort-redis-streams-client/redis"
    "github.com/bohunn/gort-trade-model"
)

func main() {
    config := redis.Config{
        Host:     "localhost",
        Port:     6379,
        Password: "",
        DB:       0,
    }

    client, err := redis.NewStreamClient(config)
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Client is ready to use!
}
```

### 3. Publish Data

```go
// Publish liquidation event
liquidation := &models.LiquidationEvent{
    Exchange:  models.ExchangeBinance,
    Symbol:    models.SymbolBTCUSDT,
    Timestamp: time.Now().UnixMilli(),
    Side:      models.SideLong,
    Price:     45000.0,
    Quantity:  1.5,
    Value:     67500.0,
    OrderType: models.OrderTypeLiquidation,
}

err := client.PublishLiquidation(liquidation)
if err != nil {
    log.Printf("Failed to publish: %v", err)
}
```

### 4. Consume Data

```go
config := redis.ConsumerConfig{
    Group:     "my-group",
    Consumer:  "my-consumer",
    Streams:   []string{"liquidations:binance:BTCUSDT"},
    BatchSize: 10,
    BlockMs:   5000,
}

err := client.ConsumeStreams(config, func(stream string, msg redis.XMessage) error {
    // Parse and process the message
    event, err := redis.ParseLiquidationMessage(msg)
    if err != nil {
        return err
    }
    
    fmt.Printf("Liquidation: %s %s $%.2f\n", event.Side, event.Symbol, event.Price)
    return nil
})
```

## Stream Organization

The client automatically organizes streams by data type, exchange, and symbol:

- **Liquidations**: `liquidations:{exchange}:{symbol}`
- **Market Data**: `market:{exchange}:{symbol}`
- **Positions**: `positions:{exchange}:{symbol}`
- **Order Books**: `orderbook:{exchange}:{symbol}`
- **Heatmaps**: `heatmap:{symbol}`

### Examples:
- `liquidations:binance:BTCUSDT`
- `market:okx:ETHUSDT`
- `heatmap:BTCUSDT`

## API Reference

### Client Methods

#### Publishing

```go
// Publish liquidation event
PublishLiquidation(event *models.LiquidationEvent) error

// Publish market snapshot
PublishMarketSnapshot(snapshot *models.MarketSnapshot) error

// Publish position distribution
PublishPositionDistribution(pos *models.PositionDistribution) error

// Publish order book snapshot
PublishOrderBook(ob *models.OrderBookSnapshot) error
```

#### Consuming

```go
// Start consuming from multiple streams
ConsumeStreams(cfg ConsumerConfig, handler func(string, redis.XMessage) error) error

// Parse liquidation message
ParseLiquidationMessage(msg redis.XMessage) (*models.LiquidationEvent, error)

// Parse market snapshot message
ParseMarketMessage(msg redis.XMessage) (*models.MarketSnapshot, error)
```

#### Caching

```go
// Cache heatmap data with TTL
CacheHeatmap(heatmap *models.HeatmapData) error

// Retrieve cached heatmap
GetCachedHeatmap(symbol models.Symbol, interval models.Interval) (*models.HeatmapData, error)
```

#### Monitoring

```go
// Get stream information
GetStreamInfo(streamName string) (*redis.XInfoStream, error)

// Get consumer group info
GetConsumerGroupInfo(streamName string) ([]redis.XInfoGroup, error)

// Get stream length
GetStreamLength(streamName string) (int64, error)

// Get pending messages
GetPendingMessages(streamName, group string) (*redis.XPending, error)
```

#### Cleanup

```go
// Trim stream to maximum length
TrimStream(streamName string, maxLen int64) error

// Delete old messages
DeleteOldMessages(streamName string, olderThan time.Duration) error
```

## Consumer Groups

The client supports multiple consumer groups for different services:

- **aggregator-group**: For aggregation services
- **storage-group**: For persistent storage
- **monitor-group**: For monitoring and alerting
- **api-group**: For API services

## Configuration

### Redis Config

```go
type Config struct {
    Host     string // Redis host (default: localhost)
    Port     int    // Redis port (default: 6379)
    Password string // Redis password (optional)
    DB       int    // Redis database (default: 0)
}
```

### Consumer Config

```go
type ConsumerConfig struct {
    Group     string   // Consumer group name
    Consumer  string   // Consumer name (unique within group)
    Streams   []string // Stream names to consume from
    BatchSize int64    // Messages per batch (default: 10)
    BlockMs   int64    // Block timeout in ms (default: 5000)
}
```

## Stream Limits

Automatic stream trimming is configured per data type:

- **Liquidations**: 10,000 messages
- **Market Data**: 1,000 messages
- **Positions**: 5,000 messages
- **Order Books**: 100 messages
- **Heatmaps**: 100 messages

## Cache TTL

Heatmap cache TTL varies by interval:

- **1m**: 5 minutes
- **5m**: 15 minutes
- **15m**: 30 minutes
- **1h**: 2 hours
- **4h**: 8 hours
- **1d**: 48 hours

## Testing

Run tests with Redis available:

```bash
# Start Redis (Docker)
docker run -d -p 6379:6379 redis:7-alpine

# Run tests
go test ./...

# Run tests with coverage
go test -cover ./...
```

Skip Redis-dependent tests:
```bash
# Tests will be skipped if Redis is not available
go test ./...
```

## Examples

See [examples/basic_usage.go](examples/basic_usage.go) for comprehensive usage examples.

## Monitoring Commands

Useful Redis CLI commands for monitoring:

```bash
# View stream information
redis-cli XINFO STREAM liquidations:binance:BTCUSDT

# View consumer groups
redis-cli XINFO GROUPS liquidations:binance:BTCUSDT

# View last 5 messages
redis-cli XREVRANGE liquidations:binance:BTCUSDT + - COUNT 5

# Monitor in real-time
redis-cli XREAD BLOCK 0 STREAMS liquidations:binance:BTCUSDT $

# View pending messages
redis-cli XPENDING liquidations:binance:BTCUSDT aggregator-group
```

## Error Handling

The client includes comprehensive error handling:

- **Connection errors**: Automatic retry with exponential backoff
- **Validation errors**: Data validation before publishing
- **Consumer errors**: Failed messages remain in pending list
- **Network errors**: Graceful degradation and logging

## Performance

- **High throughput**: Handles thousands of messages per second
- **Low latency**: Sub-millisecond publishing
- **Memory efficient**: Automatic stream trimming
- **Scalable**: Supports multiple consumers per group

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details.

## Related Projects

- [gort-trade-model](https://github.com/bohunn/gort-trade-model) - Data structures for crypto trading