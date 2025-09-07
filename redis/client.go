// Package redis provides Redis Streams integration for the liquidation heatmap service
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bohunn/gort-trade-model/models"
	"github.com/redis/go-redis/v9"
)

// StreamClient handles Redis Streams operations
type StreamClient struct {
	client *redis.Client
	ctx    context.Context
}

// Config holds Redis configuration
type Config struct {
	Host     string
	Port     int
	Password string
	DB       int
}

// NewStreamClient creates a new Redis Streams client
func NewStreamClient(cfg Config) (*StreamClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx := context.Background()

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &StreamClient{
		client: client,
		ctx:    ctx,
	}, nil
}

// Close closes the Redis connection
func (s *StreamClient) Close() error {
	return s.client.Close()
}

// ===========================================
// PUBLISHER METHODS (Exchange Services)
// ===========================================

// PublishLiquidation publishes a liquidation event to the stream
func (s *StreamClient) PublishLiquidation(event *models.LiquidationEvent) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("invalid liquidation event: %w", err)
	}

	streamName := models.GetLiquidationStreamName(event.Exchange, event.Symbol)

	args := &redis.XAddArgs{
		Stream: streamName,
		MaxLen: 10000, // Keep last 10k events
		Approx: true,  // Use ~ for approximate trimming (more efficient)
		Values: map[string]interface{}{
			"timestamp":  event.Timestamp,
			"side":       string(event.Side),
			"price":      event.Price,
			"quantity":   event.Quantity,
			"value":      event.Value,
			"order_type": string(event.OrderType),
		},
	}

	id, err := s.client.XAdd(s.ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to publish liquidation: %w", err)
	}

	log.Printf("Published liquidation to %s with ID: %s", streamName, id)
	return nil
}

// PublishMarketSnapshot publishes market data to the stream
func (s *StreamClient) PublishMarketSnapshot(snapshot *models.MarketSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("invalid market snapshot: %w", err)
	}

	streamName := models.GetMarketStreamName(snapshot.Exchange, snapshot.Symbol)

	args := &redis.XAddArgs{
		Stream: streamName,
		MaxLen: 1000,
		Approx: true,
		Values: map[string]interface{}{
			"timestamp":         snapshot.Timestamp,
			"mark_price":        snapshot.MarkPrice,
			"index_price":       snapshot.IndexPrice,
			"funding_rate":      snapshot.FundingRate,
			"open_interest":     snapshot.OpenInterest,
			"volume_24h":        snapshot.Volume24h,
			"next_funding_time": snapshot.NextFundingTime,
		},
	}

	_, err := s.client.XAdd(s.ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to publish market snapshot: %w", err)
	}

	return nil
}

// PublishPositionDistribution publishes position data to the stream
func (s *StreamClient) PublishPositionDistribution(pos *models.PositionDistribution) error {
	if err := pos.Validate(); err != nil {
		return fmt.Errorf("invalid position distribution: %w", err)
	}

	streamName := models.GetPositionStreamName(pos.Exchange, pos.Symbol)

	// Serialize complex fields to JSON
	longPos, _ := json.Marshal(pos.LongPositions)
	shortPos, _ := json.Marshal(pos.ShortPositions)

	args := &redis.XAddArgs{
		Stream: streamName,
		MaxLen: 5000,
		Approx: true,
		Values: map[string]interface{}{
			"timestamp":       pos.Timestamp,
			"price_level":     pos.PriceLevel,
			"long_positions":  string(longPos),
			"short_positions": string(shortPos),
		},
	}

	_, err := s.client.XAdd(s.ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to publish position distribution: %w", err)
	}

	return nil
}

// PublishOrderBook publishes order book snapshot to the stream
func (s *StreamClient) PublishOrderBook(ob *models.OrderBookSnapshot) error {
	streamName := models.GetOrderBookStreamName(ob.Exchange, ob.Symbol)

	// Serialize bids and asks
	bids, _ := json.Marshal(ob.Bids)
	asks, _ := json.Marshal(ob.Asks)

	args := &redis.XAddArgs{
		Stream: streamName,
		MaxLen: 100, // Keep only recent order books
		Approx: true,
		Values: map[string]interface{}{
			"timestamp":   ob.Timestamp,
			"bids":        string(bids),
			"asks":        string(asks),
			"sequence_id": ob.SequenceID,
		},
	}

	_, err := s.client.XAdd(s.ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to publish order book: %w", err)
	}

	return nil
}

// ===========================================
// CONSUMER METHODS (Aggregation Service)
// ===========================================

// ConsumerConfig holds consumer configuration
type ConsumerConfig struct {
	Group     string
	Consumer  string
	Streams   []string
	BatchSize int64
	BlockMs   int64
}

// ConsumeStreams starts consuming from multiple streams
func (s *StreamClient) ConsumeStreams(cfg ConsumerConfig, handler func(stream string, msg redis.XMessage) error) error {
	// Create consumer groups if they don't exist
	for _, stream := range cfg.Streams {
		err := s.client.XGroupCreateMkStream(s.ctx, stream, cfg.Group, "$").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			log.Printf("Warning: failed to create consumer group for %s: %v", stream, err)
		}
	}

	// Build stream arguments for XREADGROUP
	streamArgs := make([]string, 0, len(cfg.Streams)*2)
	for _, stream := range cfg.Streams {
		streamArgs = append(streamArgs, stream)
	}
	for range cfg.Streams {
		streamArgs = append(streamArgs, ">") // Read new messages
	}

	log.Printf("Starting consumer %s in group %s for streams: %v", cfg.Consumer, cfg.Group, cfg.Streams)

	for {
		// Read from streams
		args := &redis.XReadGroupArgs{
			Group:    cfg.Group,
			Consumer: cfg.Consumer,
			Streams:  streamArgs,
			Count:    cfg.BatchSize,
			Block:    time.Duration(cfg.BlockMs) * time.Millisecond,
			NoAck:    false,
		}

		result, err := s.client.XReadGroup(s.ctx, args).Result()
		if err != nil {
			if err == redis.Nil {
				continue // No new messages, continue blocking
			}
			log.Printf("Error reading from streams: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Process messages
		for _, stream := range result {
			for _, msg := range stream.Messages {
				if err := handler(stream.Stream, msg); err != nil {
					log.Printf("Error processing message %s from %s: %v", msg.ID, stream.Stream, err)
					// Don't ACK on error, message will be redelivered
					continue
				}

				// ACK the message
				if err := s.client.XAck(s.ctx, stream.Stream, cfg.Group, msg.ID).Err(); err != nil {
					log.Printf("Error ACKing message %s: %v", msg.ID, err)
				}
			}
		}
	}
}

// ParseLiquidationMessage parses a liquidation message from Redis
func ParseLiquidationMessage(msg redis.XMessage) (*models.LiquidationEvent, error) {
	event := &models.LiquidationEvent{}

	if v, ok := msg.Values["timestamp"].(string); ok {
		fmt.Sscanf(v, "%d", &event.Timestamp)
	}
	if v, ok := msg.Values["side"].(string); ok {
		event.Side = models.Side(v)
	}
	if v, ok := msg.Values["price"].(string); ok {
		fmt.Sscanf(v, "%f", &event.Price)
	}
	if v, ok := msg.Values["quantity"].(string); ok {
		fmt.Sscanf(v, "%f", &event.Quantity)
	}
	if v, ok := msg.Values["value"].(string); ok {
		fmt.Sscanf(v, "%f", &event.Value)
	}
	if v, ok := msg.Values["order_type"].(string); ok {
		event.OrderType = models.OrderType(v)
	}

	return event, nil
}

// ParseMarketMessage parses a market snapshot message from Redis
func ParseMarketMessage(msg redis.XMessage) (*models.MarketSnapshot, error) {
	snapshot := &models.MarketSnapshot{}

	if v, ok := msg.Values["timestamp"].(string); ok {
		fmt.Sscanf(v, "%d", &snapshot.Timestamp)
	}
	if v, ok := msg.Values["mark_price"].(string); ok {
		fmt.Sscanf(v, "%f", &snapshot.MarkPrice)
	}
	if v, ok := msg.Values["index_price"].(string); ok {
		fmt.Sscanf(v, "%f", &snapshot.IndexPrice)
	}
	if v, ok := msg.Values["funding_rate"].(string); ok {
		fmt.Sscanf(v, "%f", &snapshot.FundingRate)
	}
	if v, ok := msg.Values["open_interest"].(string); ok {
		fmt.Sscanf(v, "%f", &snapshot.OpenInterest)
	}
	if v, ok := msg.Values["volume_24h"].(string); ok {
		fmt.Sscanf(v, "%f", &snapshot.Volume24h)
	}
	if v, ok := msg.Values["next_funding_time"].(string); ok {
		fmt.Sscanf(v, "%d", &snapshot.NextFundingTime)
	}

	return snapshot, nil
}

// ===========================================
// CACHE METHODS
// ===========================================

// CacheHeatmap stores heatmap data in cache
func (s *StreamClient) CacheHeatmap(heatmap *models.HeatmapData) error {
	key := fmt.Sprintf("heatmap:%s:%s", heatmap.Symbol, heatmap.Interval)

	data, err := json.Marshal(heatmap)
	if err != nil {
		return fmt.Errorf("failed to marshal heatmap: %w", err)
	}

	// Cache with TTL based on interval
	ttl := getTTLForInterval(heatmap.Interval)

	if err := s.client.Set(s.ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache heatmap: %w", err)
	}

	// Also publish to heatmap stream for real-time subscribers
	streamName := models.GetHeatmapStreamName(heatmap.Symbol)
	args := &redis.XAddArgs{
		Stream: streamName,
		MaxLen: 100,
		Approx: true,
		Values: map[string]interface{}{
			"data":      string(data),
			"timestamp": heatmap.Timestamp,
			"interval":  string(heatmap.Interval),
		},
	}

	s.client.XAdd(s.ctx, args)

	return nil
}

// GetCachedHeatmap retrieves cached heatmap data
func (s *StreamClient) GetCachedHeatmap(symbol models.Symbol, interval models.Interval) (*models.HeatmapData, error) {
	key := fmt.Sprintf("heatmap:%s:%s", symbol, interval)

	data, err := s.client.Get(s.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // Cache miss
		}
		return nil, fmt.Errorf("failed to get cached heatmap: %w", err)
	}

	var heatmap models.HeatmapData
	if err := json.Unmarshal([]byte(data), &heatmap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal heatmap: %w", err)
	}

	return &heatmap, nil
}

// getTTLForInterval returns appropriate TTL for each interval
func getTTLForInterval(interval models.Interval) time.Duration {
	switch interval {
	case models.Interval1m:
		return 5 * time.Minute
	case models.Interval5m:
		return 15 * time.Minute
	case models.Interval15m:
		return 30 * time.Minute
	case models.Interval1h:
		return 2 * time.Hour
	case models.Interval4h:
		return 8 * time.Hour
	case models.Interval1d:
		return 48 * time.Hour
	default:
		return 5 * time.Minute
	}
}

// ===========================================
// MONITORING METHODS
// ===========================================

// GetStreamInfo returns information about a stream
func (s *StreamClient) GetStreamInfo(streamName string) (*redis.XInfoStream, error) {
	return s.client.XInfoStream(s.ctx, streamName).Result()
}

// GetConsumerGroupInfo returns information about consumer groups
func (s *StreamClient) GetConsumerGroupInfo(streamName string) ([]redis.XInfoGroup, error) {
	return s.client.XInfoGroups(s.ctx, streamName).Result()
}

// GetStreamLength returns the number of messages in a stream
func (s *StreamClient) GetStreamLength(streamName string) (int64, error) {
	return s.client.XLen(s.ctx, streamName).Result()
}

// GetPendingMessages returns pending messages for a consumer group
func (s *StreamClient) GetPendingMessages(streamName, group string) (*redis.XPending, error) {
	return s.client.XPending(s.ctx, streamName, group).Result()
}

// ===========================================
// CLEANUP METHODS
// ===========================================

// TrimStream trims a stream to a maximum length
func (s *StreamClient) TrimStream(streamName string, maxLen int64) error {
	return s.client.XTrimMaxLen(s.ctx, streamName, maxLen).Err()
}

// DeleteOldMessages deletes messages older than specified duration
func (s *StreamClient) DeleteOldMessages(streamName string, olderThan time.Duration) error {
	// Calculate the timestamp
	cutoff := time.Now().Add(-olderThan).UnixMilli()

	// Get messages older than cutoff
	messages, err := s.client.XRange(s.ctx, streamName, "-", fmt.Sprintf("%d", cutoff)).Result()
	if err != nil {
		return err
	}

	// Delete old messages
	messageIDs := make([]string, len(messages))
	for i, msg := range messages {
		messageIDs[i] = msg.ID
	}

	if len(messageIDs) > 0 {
		return s.client.XDel(s.ctx, streamName, messageIDs...).Err()
	}

	return nil
}
