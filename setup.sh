#!/bin/bash

# Redis Streams Setup Script for Liquidation Heatmap Service
# This script initializes Redis Streams and consumer groups

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"

# Function to execute Redis commands
redis_exec() {
    if [ -z "$REDIS_PASSWORD" ]; then
        redis-cli -h $REDIS_HOST -p $REDIS_PORT "$@"
    else
        redis-cli -h $REDIS_HOST -p $REDIS_PORT -a $REDIS_PASSWORD "$@"
    fi
}

echo "🚀 Setting up Redis Streams for Liquidation Heatmap Service"
echo "================================================"
echo "Redis Host: $REDIS_HOST:$REDIS_PORT"
echo ""

# ============================================
# Stream Names Configuration
# ============================================

EXCHANGES=("binance" "okx" "bybit" "coinbase" "kraken" "deribit" "bitfinex")
SYMBOLS=("BTCUSDT" "ETHUSDT" "BTCUSD" "ETHUSD")
DATA_TYPES=("liquidations" "positions" "market" "orderbook")

# ============================================
# Create Consumer Groups
# ============================================

echo "📌 Creating Consumer Groups..."

for exchange in "${EXCHANGES[@]}"; do
    for symbol in "${SYMBOLS[@]}"; do
        for data_type in "${DATA_TYPES[@]}"; do
            stream_name="${data_type}:${exchange}:${symbol}"

            # Create consumer group for aggregation service
            redis_exec XGROUP CREATE $stream_name "aggregator-group" $ MKSTREAM 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "  ✅ Created group 'aggregator-group' for $stream_name"
            else
                echo "  ⚠️  Group 'aggregator-group' already exists for $stream_name"
            fi

            # Create consumer group for storage service (Phase 2)
            redis_exec XGROUP CREATE $stream_name "storage-group" $ MKSTREAM 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "  ✅ Created group 'storage-group' for $stream_name"
            fi

            # Create consumer group for monitoring (Phase 2)
            redis_exec XGROUP CREATE $stream_name "monitor-group" $ MKSTREAM 2>/dev/null
            if [ $? -eq 0 ]; then
                echo "  ✅ Created group 'monitor-group' for $stream_name"
            fi
        done
    done
done

# ============================================
# Create Heatmap Streams
# ============================================

echo ""
echo "📊 Creating Heatmap Streams..."

for symbol in "${SYMBOLS[@]}"; do
    stream_name="heatmap:${symbol}"

    redis_exec XGROUP CREATE $stream_name "api-group" $ MKSTREAM 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "  ✅ Created heatmap stream: $stream_name"
    else
        echo "  ⚠️  Heatmap stream already exists: $stream_name"
    fi
done

# ============================================
# Set Stream Configuration
# ============================================

echo ""
echo "⚙️  Configuring Stream Limits..."

# Set maximum length for different stream types
for exchange in "${EXCHANGES[@]}"; do
    for symbol in "${SYMBOLS[@]}"; do
        # Liquidations: Keep last 10,000 events
        redis_exec XADD "liquidations:${exchange}:${symbol}" MAXLEN ~ 10000 "*" init "true" > /dev/null 2>&1
        redis_exec XDEL "liquidations:${exchange}:${symbol}" "*" > /dev/null 2>&1

        # Positions: Keep last 5,000 snapshots
        redis_exec XADD "positions:${exchange}:${symbol}" MAXLEN ~ 5000 "*" init "true" > /dev/null 2>&1
        redis_exec XDEL "positions:${exchange}:${symbol}" "*" > /dev/null 2>&1

        # Market: Keep last 1,000 snapshots
        redis_exec XADD "market:${exchange}:${symbol}" MAXLEN ~ 1000 "*" init "true" > /dev/null 2>&1
        redis_exec XDEL "market:${exchange}:${symbol}" "*" > /dev/null 2>&1

        # Order book: Keep last 100 snapshots
        redis_exec XADD "orderbook:${exchange}:${symbol}" MAXLEN ~ 100 "*" init "true" > /dev/null 2>&1
        redis_exec XDEL "orderbook:${exchange}:${symbol}" "*" > /dev/null 2>&1
    done
done

echo "  ✅ Stream limits configured"

# ============================================
# Test Stream Publishing
# ============================================

echo ""
echo "🧪 Testing Stream Publishing..."

# Publish test liquidation event
TEST_STREAM="liquidations:binance:BTCUSDT"
TIMESTAMP=$(date +%s%3N)

redis_exec XADD $TEST_STREAM "*" \
    timestamp "$TIMESTAMP" \
    side "long" \
    price "95000" \
    quantity "1.5" \
    value "142500" \
    order_type "liquidation" > /dev/null

if [ $? -eq 0 ]; then
    echo "  ✅ Test message published to $TEST_STREAM"
else
    echo "  ❌ Failed to publish test message"
fi

# ============================================
# Stream Information
# ============================================

echo ""
echo "📊 Stream Statistics:"
echo "===================="

# Count total streams
total_streams=$(redis_exec --scan --pattern "*:*:*" | wc -l)
echo "  Total Streams: $total_streams"

# Show sample stream info
echo ""
echo "  Sample Stream Info (liquidations:binance:BTCUSDT):"
redis_exec XINFO STREAM liquidations:binance:BTCUSDT 2>/dev/null || echo "    Stream not yet populated"

# ============================================
# Redis CLI Commands Reference
# ============================================

cat << 'EOF'

📚 Useful Redis Commands Reference:
====================================

# View stream information:
XINFO STREAM liquidations:binance:BTCUSDT

# View consumer groups:
XINFO GROUPS liquidations:binance:BTCUSDT

# View consumers in a group:
XINFO CONSUMERS liquidations:binance:BTCUSDT aggregator-group

# Read last 5 messages:
XREVRANGE liquidations:binance:BTCUSDT + - COUNT 5

# Monitor stream in real-time:
XREAD BLOCK 0 STREAMS liquidations:binance:BTCUSDT $

# View pending messages:
XPENDING liquidations:binance:BTCUSDT aggregator-group

# Claim abandoned messages (after 30 minutes):
XAUTOCLAIM liquidations:binance:BTCUSDT aggregator-group consumer-1 1800000 0-0

# Trim stream to last 1000 messages:
XTRIM liquidations:binance:BTCUSDT MAXLEN ~ 1000

# Delete specific messages:
XDEL liquidations:binance:BTCUSDT 1234567890-0

# Reset consumer group to beginning:
XGROUP SETID liquidations:binance:BTCUSDT aggregator-group 0

EOF

echo ""
echo "✅ Redis Streams setup complete!"
echo ""
echo "🎯 Next Steps:"
echo "  1. Start exchange connector services"
echo "  2. Start aggregation service"
echo "  3. Monitor streams with: redis-cli XINFO STREAM <stream-name>"
echo ""