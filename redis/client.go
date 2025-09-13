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

	// Configuration
	streamMaxLen    int64
	heatmapCacheTTL time.Duration
}

// Config holds Redis configuration
type Config struct {
	Host            string
	Port            int
	Password        string
	DB              int
	StreamMaxLen    int64         // Max messages per stream (default: 10000)
	HeatmapCacheTTL time.Duration // TTL for cached heatmaps (default: 5 minutes)
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

	// Set defaults
	streamMaxLen := cfg.StreamMaxLen
	if streamMaxLen == 0 {
		streamMaxLen = 10000
	}

	heatmapCacheTTL := cfg.HeatmapCacheTTL
	if heatmapCacheTTL == 0 {
		heatmapCacheTTL = 5 * time.Minute
	}

	return &StreamClient{
		client:          client,
		ctx:             ctx,
		streamMaxLen:    streamMaxLen,
		heatmapCacheTTL: heatmapCacheTTL,
	}, nil
}

// Close closes the Redis connection
func (s *StreamClient) Close() error {
	return s.client.Close()
}

// ===========================================
// LIQUIDATION EVENT PUBLISHING
// ===========================================

// PublishLiquidation publishes a liquidation event to the stream
func (s *StreamClient) PublishLiquidation(event *models.LiquidationEvent) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("invalid liquidation event: %w", err)
	}

	streamName := models.GetLiquidationStreamName(event.Exchange, event.Symbol)

	args := &redis.XAddArgs{
		Stream: streamName,
		MaxLen: s.streamMaxLen,
		Approx: true, // Use ~ for approximate trimming (more efficient)
		Values: map[string]interface{}{
			"timestamp":        event.Timestamp,
			"side":             string(event.Side),
			"price":            event.Price,
			"quantity":         event.Quantity,
			"value":            event.Value,
			"order_type":       string(event.OrderType),
			"avg_price":        event.AvgPrice,
			"filled_qty":       event.FilledQty,
			"order_status":     event.OrderStatus,
			"order_trade_time": event.OrderTradeTime,
		},
	}

	id, err := s.client.XAdd(s.ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to publish liquidation: %w", err)
	}

	log.Printf("[Redis] Published liquidation to %s with ID: %s", streamName, id)
	return nil
}

// PublishLiquidationBatch publishes multiple liquidation events efficiently
func (s *StreamClient) PublishLiquidationBatch(events []*models.LiquidationEvent) error {
	pipe := s.client.Pipeline()

	for _, event := range events {
		if err := event.Validate(); err != nil {
			log.Printf("[Redis] Skipping invalid liquidation: %v", err)
			continue
		}

		streamName := models.GetLiquidationStreamName(event.Exchange, event.Symbol)

		pipe.XAdd(s.ctx, &redis.XAddArgs{
			Stream: streamName,
			MaxLen: s.streamMaxLen,
			Approx: true,
			Values: map[string]interface{}{
				"timestamp":        event.Timestamp,
				"side":             string(event.Side),
				"price":            event.Price,
				"quantity":         event.Quantity,
				"value":            event.Value,
				"order_type":       string(event.OrderType),
				"avg_price":        event.AvgPrice,
				"filled_qty":       event.FilledQty,
				"order_status":     event.OrderStatus,
				"order_trade_time": event.OrderTradeTime,
			},
		})
	}

	_, err := pipe.Exec(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to publish liquidation batch: %w", err)
	}

	log.Printf("[Redis] Published batch of %d liquidations", len(events))
	return nil
}

// ===========================================
// MARKET DATA PUBLISHING
// ===========================================

// PublishMarketSnapshot publishes market data to the stream
func (s *StreamClient) PublishMarketSnapshot(snapshot *models.MarketSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("invalid market snapshot: %w", err)
	}

	streamName := models.GetMarketStreamName(snapshot.Exchange, snapshot.Symbol)

	args := &redis.XAddArgs{
		Stream: streamName,
		MaxLen: 1000, // Keep less market snapshots
		Approx: true,
		Values: map[string]interface{}{
			"timestamp":         snapshot.Timestamp,
			"mark_price":        snapshot.MarkPrice,
			"index_price":       snapshot.IndexPrice,
			"funding_rate":      snapshot.FundingRate,
			"open_interest":     snapshot.OpenInterest,
			"open_interest_usd": snapshot.OpenInterestUSD,
			"volume_24h":        snapshot.Volume24h,
			"turnover_24h":      snapshot.Turnover24h,
			"next_funding_time": snapshot.NextFundingTime,
		},
	}

	_, err := s.client.XAdd(s.ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to publish market snapshot: %w", err)
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
			"timestamp":      ob.Timestamp,
			"bids":           string(bids),
			"asks":           string(asks),
			"last_update_id": ob.LastUpdateID,
			"spread":         ob.Spread,
			"mid_price":      ob.MidPrice,
			"imbalance":      ob.Imbalance,
		},
	}

	_, err := s.client.XAdd(s.ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to publish order book: %w", err)
	}

	return nil
}

// ===========================================
// HEATMAP DATA CACHING & STREAMING
// ===========================================

// CacheAndPublishHeatmap stores heatmap in cache AND publishes to stream
func (s *StreamClient) CacheAndPublishHeatmap(heatmap *models.HeatmapData) error {
	if err := heatmap.Validate(); err != nil {
		return fmt.Errorf("invalid heatmap data: %w", err)
	}

	// 1. Cache the heatmap for quick retrieval
	if err := s.cacheHeatmap(heatmap); err != nil {
		log.Printf("[Redis] Warning: failed to cache heatmap: %v", err)
	}

	// 2. Publish to heatmap stream for real-time subscribers
	if err := s.publishHeatmapToStream(heatmap); err != nil {
		log.Printf("[Redis] Warning: failed to publish heatmap to stream: %v", err)
	}

	return nil
}

// cacheHeatmap stores heatmap data in cache with TTL
func (s *StreamClient) cacheHeatmap(heatmap *models.HeatmapData) error {
	// Use default interval if not specified
	interval := heatmap.Interval
	if interval == "" {
		interval = models.Interval1s
	}

	key := models.GetHeatmapCacheKey(heatmap.Symbol, interval)

	data, err := json.Marshal(heatmap)
	if err != nil {
		return fmt.Errorf("failed to marshal heatmap: %w", err)
	}

	// Cache with TTL
	ttl := s.getHeatmapTTL(interval)

	if err := s.client.Set(s.ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to cache heatmap: %w", err)
	}

	log.Printf("[Redis] Cached heatmap for %s:%s (TTL: %v)", heatmap.Symbol, interval, ttl)
	return nil
}

// publishHeatmapToStream publishes heatmap to stream for real-time subscribers
func (s *StreamClient) publishHeatmapToStream(heatmap *models.HeatmapData) error {
	streamName := models.GetHeatmapStreamName(heatmap.Symbol)

	// For stream, we'll store a lighter version with reference to cache
	// This prevents stream bloat while maintaining real-time updates

	// Serialize only essential data for stream
	summary, _ := json.Marshal(heatmap.Summary)
	clusters, _ := json.Marshal(heatmap.Clusters)

	args := &redis.XAddArgs{
		Stream: streamName,
		MaxLen: 100, // Keep only recent heatmap updates
		Approx: true,
		Values: map[string]interface{}{
			"timestamp":     heatmap.Timestamp,
			"symbol":        string(heatmap.Symbol),
			"interval":      string(heatmap.Interval),
			"current_price": heatmap.CurrentPrice,
			"summary":       string(summary),
			"clusters":      string(clusters),
			"levels_count":  len(heatmap.Levels),
			"cache_key":     models.GetHeatmapCacheKey(heatmap.Symbol, heatmap.Interval),
		},
	}

	id, err := s.client.XAdd(s.ctx, args).Result()
	if err != nil {
		return fmt.Errorf("failed to publish heatmap to stream: %w", err)
	}

	log.Printf("[Redis] Published heatmap update to %s with ID: %s", streamName, id)
	return nil
}

// GetCachedHeatmap retrieves cached heatmap data
func (s *StreamClient) GetCachedHeatmap(symbol models.Symbol, interval models.Interval) (*models.HeatmapData, error) {
	key := models.GetHeatmapCacheKey(symbol, interval)

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

// GetLatestHeatmaps retrieves the most recent heatmap for each symbol
func (s *StreamClient) GetLatestHeatmaps(symbols []models.Symbol, interval models.Interval) (map[models.Symbol]*models.HeatmapData, error) {
	result := make(map[models.Symbol]*models.HeatmapData)

	for _, symbol := range symbols {
		heatmap, err := s.GetCachedHeatmap(symbol, interval)
		if err != nil {
			log.Printf("[Redis] Failed to get heatmap for %s: %v", symbol, err)
			continue
		}
		if heatmap != nil {
			result[symbol] = heatmap
		}
	}

	return result, nil
}

// getHeatmapTTL returns appropriate TTL for each interval
func (s *StreamClient) getHeatmapTTL(interval models.Interval) time.Duration {
	// Override with configured TTL if interval is 1s (real-time)
	if interval == models.Interval1s {
		return s.heatmapCacheTTL
	}

	// Otherwise use interval-based TTL
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
		return s.heatmapCacheTTL
	}
}

// ===========================================
// CONSUMER METHODS
// ===========================================

// ConsumerConfig holds consumer configuration
type ConsumerConfig struct {
	Group     string
	Consumer  string
	Streams   []string
	BatchSize int64
	BlockMs   int64
}

// ConsumeHeatmapStream consumes heatmap updates from stream
func (s *StreamClient) ConsumeHeatmapStream(symbol models.Symbol, handler func(*models.HeatmapData) error) error {
	streamName := models.GetHeatmapStreamName(symbol)

	// Read from the stream
	for {
		result, err := s.client.XRead(s.ctx, &redis.XReadArgs{
			Streams: []string{streamName, "$"}, // Read new messages only
			Count:   10,
			Block:   0, // Block indefinitely
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}
			log.Printf("[Redis] Error reading heatmap stream: %v", err)
			time.Sleep(time.Second)
			continue
		}

		for _, stream := range result {
			for _, msg := range stream.Messages {
				// Get the full heatmap from cache using the cache key
				if cacheKey, ok := msg.Values["cache_key"].(string); ok {
					// Extract symbol and interval from cache key
					data, err := s.client.Get(s.ctx, cacheKey).Result()
					if err != nil {
						log.Printf("[Redis] Failed to get heatmap from cache: %v", err)
						continue
					}

					var heatmap models.HeatmapData
					if err := json.Unmarshal([]byte(data), &heatmap); err != nil {
						log.Printf("[Redis] Failed to unmarshal heatmap: %v", err)
						continue
					}

					if err := handler(&heatmap); err != nil {
						log.Printf("[Redis] Handler error: %v", err)
					}
				}
			}
		}
	}
}

// ConsumeLiquidationStream consumes liquidation events from stream
func (s *StreamClient) ConsumeLiquidationStream(cfg ConsumerConfig, handler func(*models.LiquidationEvent) error) error {
	// Create consumer groups if they don't exist
	for _, stream := range cfg.Streams {
		err := s.client.XGroupCreateMkStream(s.ctx, stream, cfg.Group, "$").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			log.Printf("[Redis] Warning: failed to create consumer group for %s: %v", stream, err)
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

	log.Printf("[Redis] Starting consumer %s in group %s for streams: %v", cfg.Consumer, cfg.Group, cfg.Streams)

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
				continue // No new messages
			}
			log.Printf("[Redis] Error reading from streams: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Process messages
		for _, stream := range result {
			for _, msg := range stream.Messages {
				event, err := ParseLiquidationMessage(msg)
				if err != nil {
					log.Printf("[Redis] Failed to parse liquidation: %v", err)
					continue
				}

				if err := handler(event); err != nil {
					log.Printf("[Redis] Handler error: %v", err)
					continue
				}

				// ACK the message
				if err := s.client.XAck(s.ctx, stream.Stream, cfg.Group, msg.ID).Err(); err != nil {
					log.Printf("[Redis] Error ACKing message %s: %v", msg.ID, err)
				}
			}
		}
	}
}

// ParseLiquidationMessage parses a liquidation message from Redis
func ParseLiquidationMessage(msg redis.XMessage) (*models.LiquidationEvent, error) {
	event := &models.LiquidationEvent{}

	// Parse each field
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
	if v, ok := msg.Values["avg_price"].(string); ok {
		fmt.Sscanf(v, "%f", &event.AvgPrice)
	}
	if v, ok := msg.Values["filled_qty"].(string); ok {
		fmt.Sscanf(v, "%f", &event.FilledQty)
	}
	if v, ok := msg.Values["order_status"].(string); ok {
		event.OrderStatus = v
	}
	if v, ok := msg.Values["order_trade_time"].(string); ok {
		fmt.Sscanf(v, "%d", &event.OrderTradeTime)
	}

	return event, nil
}

// ===========================================
// MONITORING & MAINTENANCE
// ===========================================

// GetStreamInfo returns information about a stream
func (s *StreamClient) GetStreamInfo(streamName string) (*redis.XInfoStream, error) {
	return s.client.XInfoStream(s.ctx, streamName).Result()
}

// GetStreamLength returns the number of messages in a stream
func (s *StreamClient) GetStreamLength(streamName string) (int64, error) {
	return s.client.XLen(s.ctx, streamName).Result()
}

// TrimStream trims a stream to a maximum length
func (s *StreamClient) TrimStream(streamName string, maxLen int64) error {
	return s.client.XTrimMaxLen(s.ctx, streamName, maxLen).Err()
}

// GetPendingMessages returns pending messages for a consumer group
func (s *StreamClient) GetPendingMessages(streamName, group string) (*redis.XPending, error) {
	return s.client.XPending(s.ctx, streamName, group).Result()
}

// HealthCheck performs a health check on Redis connection
func (s *StreamClient) HealthCheck() error {
	return s.client.Ping(s.ctx).Err()
}

// GetStats returns statistics about all relevant streams
func (s *StreamClient) GetStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// Get info for common symbols
	symbols := []models.Symbol{
		models.SymbolBTCUSDT,
		models.SymbolETHUSDT,
		models.SymbolBNBUSDT,
		models.SymbolSOLUSDT,
		models.SymbolXRPUSDT,
	}

	for _, symbol := range symbols {
		// Check liquidation stream
		liqStream := models.GetLiquidationStreamName(models.ExchangeBinance, symbol)
		if length, err := s.GetStreamLength(liqStream); err == nil {
			stats[fmt.Sprintf("liquidations_%s_count", symbol)] = length
		}

		// Check heatmap stream
		heatmapStream := models.GetHeatmapStreamName(symbol)
		if length, err := s.GetStreamLength(heatmapStream); err == nil {
			stats[fmt.Sprintf("heatmap_%s_count", symbol)] = length
		}

		// Check cached heatmap
		if heatmap, err := s.GetCachedHeatmap(symbol, models.Interval1s); err == nil && heatmap != nil {
			stats[fmt.Sprintf("heatmap_%s_cached", symbol)] = true
			stats[fmt.Sprintf("heatmap_%s_levels", symbol)] = len(heatmap.Levels)
			stats[fmt.Sprintf("heatmap_%s_clusters", symbol)] = len(heatmap.Clusters)
		}
	}

	return stats, nil
}
