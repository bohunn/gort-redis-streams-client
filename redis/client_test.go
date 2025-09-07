package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bohunn/gort-trade-model/models"
	"github.com/redis/go-redis/v9"
)

// Test helper to create a test Redis client
func setupTestClient(t *testing.T) *StreamClient {
	// Skip tests if Redis is not available
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost"
	}

	config := Config{
		Host:     redisHost,
		Port:     6379,
		Password: "",
		DB:       1, // Use DB 1 for tests
	}

	client, err := NewStreamClient(config)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Clean up test streams before each test
	client.client.FlushDB(context.Background())

	return client
}

func TestNewStreamClient(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	if client == nil {
		t.Fatal("Expected non-nil client")
	}
}

func TestPublishLiquidation(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	event := &models.LiquidationEvent{
		Exchange:  models.ExchangeBinance,
		Symbol:    models.SymbolBTCUSDT,
		Timestamp: time.Now().UnixMilli(),
		Side:      models.SideLong,
		Price:     45000.0,
		Quantity:  1.5,
		Value:     67500.0,
		OrderType: models.OrderTypeLiquidation,
	}

	err := client.PublishLiquidation(event)
	if err != nil {
		t.Fatalf("Failed to publish liquidation: %v", err)
	}

	// Verify the message was published
	streamName := models.GetLiquidationStreamName(event.Exchange, event.Symbol)
	length, err := client.GetStreamLength(streamName)
	if err != nil {
		t.Fatalf("Failed to get stream length: %v", err)
	}

	if length != 1 {
		t.Errorf("Expected stream length 1, got %d", length)
	}
}

func TestPublishMarketSnapshot(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	snapshot := &models.MarketSnapshot{
		Exchange:        models.ExchangeBinance,
		Symbol:          models.SymbolBTCUSDT,
		Timestamp:       time.Now().UnixMilli(),
		MarkPrice:       45000.0,
		IndexPrice:      44980.0,
		FundingRate:     0.0001,
		OpenInterest:    1000000000,
		Volume24h:       500000000,
		NextFundingTime: time.Now().Add(4 * time.Hour).UnixMilli(),
	}

	err := client.PublishMarketSnapshot(snapshot)
	if err != nil {
		t.Fatalf("Failed to publish market snapshot: %v", err)
	}

	// Verify the message was published
	streamName := models.GetMarketStreamName(snapshot.Exchange, snapshot.Symbol)
	length, err := client.GetStreamLength(streamName)
	if err != nil {
		t.Fatalf("Failed to get stream length: %v", err)
	}

	if length != 1 {
		t.Errorf("Expected stream length 1, got %d", length)
	}
}

func TestParseLiquidationMessage(t *testing.T) {
	// Create a mock Redis message
	msg := redis.XMessage{
		ID: "1234567890-0",
		Values: map[string]interface{}{
			"timestamp":  "1234567890",
			"side":       "long",
			"price":      "45000.0",
			"quantity":   "1.5",
			"value":      "67500.0",
			"order_type": "liquidation",
		},
	}

	event, err := ParseLiquidationMessage(msg)
	if err != nil {
		t.Fatalf("Failed to parse liquidation message: %v", err)
	}

	if event.Timestamp != 1234567890 {
		t.Errorf("Expected timestamp 1234567890, got %d", event.Timestamp)
	}

	if event.Side != models.SideLong {
		t.Errorf("Expected side %s, got %s", models.SideLong, event.Side)
	}

	if event.Price != 45000.0 {
		t.Errorf("Expected price 45000.0, got %f", event.Price)
	}
}

func TestConsumeStreams(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	// First, publish a test message
	event := &models.LiquidationEvent{
		Exchange:  models.ExchangeBinance,
		Symbol:    models.SymbolBTCUSDT,
		Timestamp: time.Now().UnixMilli(),
		Side:      models.SideLong,
		Price:     45000.0,
		Quantity:  1.5,
		Value:     67500.0,
		OrderType: models.OrderTypeLiquidation,
	}

	err := client.PublishLiquidation(event)
	if err != nil {
		t.Fatalf("Failed to publish test liquidation: %v", err)
	}

	// Now test consuming
	streamName := models.GetLiquidationStreamName(event.Exchange, event.Symbol)

	config := ConsumerConfig{
		Group:     "test-group",
		Consumer:  "test-consumer",
		Streams:   []string{streamName},
		BatchSize: 10,
		BlockMs:   1000, // 1 second timeout for test
	}

	messageReceived := false
	done := make(chan bool, 1)

	// Start consumer in a goroutine
	go func() {
		defer func() {
			done <- true
		}()

		err := client.ConsumeStreams(config, func(stream string, msg redis.XMessage) error {
			if stream == streamName {
				messageReceived = true
				// Return an error to stop the consumer for test
				return context.Canceled
			}
			return nil
		})

		// ConsumeStreams should return an error when handler returns an error
		if err == nil {
			t.Errorf("Expected ConsumeStreams to return error from handler")
		}
	}()

	// Wait a bit for consumer to process
	select {
	case <-done:
		// Consumer finished
	case <-time.After(5 * time.Second):
		t.Fatal("Consumer test timed out")
	}

	if !messageReceived {
		t.Error("Expected to receive message from stream")
	}
}

func TestCacheHeatmap(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	heatmap := &models.HeatmapData{
		Symbol:       models.SymbolBTCUSDT,
		Timestamp:    time.Now().UnixMilli(),
		Interval:     models.Interval1m,
		CurrentPrice: 45000.0,
		LiquidationLevels: []models.LiquidationLevel{
			{
				Price:                       44000.0,
				CumulativeLongLiquidations:  1000000.0,
				CumulativeShortLiquidations: 500000.0,
				EstimatedImpact:             2.5,
				Exchanges:                   []models.Exchange{models.ExchangeBinance},
			},
		},
		Metadata: models.HeatmapMetadata{
			ExchangesCovered: []models.Exchange{models.ExchangeBinance},
			DataCompleteness: 95.0,
			CalculationTime:  150,
			LastUpdate:       time.Now().UnixMilli(),
		},
	}

	// Test caching
	err := client.CacheHeatmap(heatmap)
	if err != nil {
		t.Fatalf("Failed to cache heatmap: %v", err)
	}

	// Test retrieval
	cached, err := client.GetCachedHeatmap(heatmap.Symbol, heatmap.Interval)
	if err != nil {
		t.Fatalf("Failed to get cached heatmap: %v", err)
	}

	if cached == nil {
		t.Fatal("Expected cached heatmap, got nil")
	}

	if cached.Symbol != heatmap.Symbol {
		t.Errorf("Expected symbol %s, got %s", heatmap.Symbol, cached.Symbol)
	}

	if cached.CurrentPrice != heatmap.CurrentPrice {
		t.Errorf("Expected current price %f, got %f", heatmap.CurrentPrice, cached.CurrentPrice)
	}
}

func TestStreamMonitoring(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	// Publish a test message first
	event := &models.LiquidationEvent{
		Exchange:  models.ExchangeBinance,
		Symbol:    models.SymbolBTCUSDT,
		Timestamp: time.Now().UnixMilli(),
		Side:      models.SideLong,
		Price:     45000.0,
		Quantity:  1.5,
		Value:     67500.0,
		OrderType: models.OrderTypeLiquidation,
	}

	err := client.PublishLiquidation(event)
	if err != nil {
		t.Fatalf("Failed to publish liquidation: %v", err)
	}

	streamName := models.GetLiquidationStreamName(event.Exchange, event.Symbol)

	// Test stream info
	info, err := client.GetStreamInfo(streamName)
	if err != nil {
		t.Fatalf("Failed to get stream info: %v", err)
	}

	if info.Length != 1 {
		t.Errorf("Expected stream length 1, got %d", info.Length)
	}

	// Test stream length
	length, err := client.GetStreamLength(streamName)
	if err != nil {
		t.Fatalf("Failed to get stream length: %v", err)
	}

	if length != 1 {
		t.Errorf("Expected stream length 1, got %d", length)
	}
}

func TestTrimStream(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	streamName := "test:trim:stream"

	// Add multiple messages
	for i := 0; i < 10; i++ {
		err := client.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: streamName,
			Values: map[string]interface{}{
				"data": i,
			},
		}).Err()
		if err != nil {
			t.Fatalf("Failed to add test message: %v", err)
		}
	}

	// Trim to 5 messages
	err := client.TrimStream(streamName, 5)
	if err != nil {
		t.Fatalf("Failed to trim stream: %v", err)
	}

	// Check length
	length, err := client.GetStreamLength(streamName)
	if err != nil {
		t.Fatalf("Failed to get stream length: %v", err)
	}

	if length != 5 {
		t.Errorf("Expected stream length 5 after trim, got %d", length)
	}
}
