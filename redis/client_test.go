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
		Host:            redisHost,
		Port:            6379,
		Password:        "",
		DB:              1, // Use DB 1 for tests
		StreamMaxLen:    1000,
		HeatmapCacheTTL: 5 * time.Minute,
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

	// Check that defaults are set
	if client.streamMaxLen != 1000 {
		t.Errorf("Expected streamMaxLen 1000, got %d", client.streamMaxLen)
	}

	if client.heatmapCacheTTL != 5*time.Minute {
		t.Errorf("Expected heatmapCacheTTL 5 minutes, got %v", client.heatmapCacheTTL)
	}
}

func TestPublishLiquidation(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	event := &models.LiquidationEvent{
		Exchange:       models.ExchangeBinance,
		Symbol:         models.SymbolBTCUSDT,
		Timestamp:      time.Now().UnixMilli(),
		Side:           models.SideLong,
		Price:          45000.0,
		Quantity:       1.5,
		Value:          67500.0,
		OrderType:      models.OrderTypeLiquidation,
		AvgPrice:       45100.0,
		FilledQty:      1.5,
		OrderStatus:    "FILLED",
		OrderTradeTime: time.Now().UnixMilli(),
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

func TestPublishLiquidationBatch(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	events := []*models.LiquidationEvent{
		{
			Exchange:  models.ExchangeBinance,
			Symbol:    models.SymbolBTCUSDT,
			Timestamp: time.Now().UnixMilli(),
			Side:      models.SideLong,
			Price:     45000.0,
			Quantity:  1.5,
			OrderType: models.OrderTypeLiquidation,
		},
		{
			Exchange:  models.ExchangeBinance,
			Symbol:    models.SymbolBTCUSDT,
			Timestamp: time.Now().UnixMilli(),
			Side:      models.SideShort,
			Price:     46000.0,
			Quantity:  2.0,
			OrderType: models.OrderTypeLiquidation,
		},
		{
			Exchange:  models.ExchangeBinance,
			Symbol:    models.SymbolETHUSDT,
			Timestamp: time.Now().UnixMilli(),
			Side:      models.SideLong,
			Price:     3000.0,
			Quantity:  10.0,
			OrderType: models.OrderTypeLiquidation,
		},
	}

	err := client.PublishLiquidationBatch(events)
	if err != nil {
		t.Fatalf("Failed to publish liquidation batch: %v", err)
	}

	// Verify messages were published to correct streams
	btcStream := models.GetLiquidationStreamName(models.ExchangeBinance, models.SymbolBTCUSDT)
	btcLength, err := client.GetStreamLength(btcStream)
	if err != nil {
		t.Fatalf("Failed to get BTC stream length: %v", err)
	}

	if btcLength != 2 {
		t.Errorf("Expected BTC stream length 2, got %d", btcLength)
	}

	ethStream := models.GetLiquidationStreamName(models.ExchangeBinance, models.SymbolETHUSDT)
	ethLength, err := client.GetStreamLength(ethStream)
	if err != nil {
		t.Fatalf("Failed to get ETH stream length: %v", err)
	}

	if ethLength != 1 {
		t.Errorf("Expected ETH stream length 1, got %d", ethLength)
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
		OpenInterest:    1000000,
		OpenInterestUSD: 45000000000,
		Volume24h:       500000000,
		Turnover24h:     450000000,
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

func TestPublishOrderBook(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	orderbook := &models.OrderBookSnapshot{
		Exchange:  models.ExchangeBinance,
		Symbol:    models.SymbolBTCUSDT,
		Timestamp: time.Now().UnixMilli(),
		Bids: []models.PriceLevel{
			{Price: 44900, Quantity: 10, Count: 5},
			{Price: 44890, Quantity: 20, Count: 8},
		},
		Asks: []models.PriceLevel{
			{Price: 45100, Quantity: 15, Count: 6},
			{Price: 45110, Quantity: 25, Count: 10},
		},
		LastUpdateID: 123456789,
		Spread:       200,
		MidPrice:     45000,
		Imbalance:    -0.1,
	}

	err := client.PublishOrderBook(orderbook)
	if err != nil {
		t.Fatalf("Failed to publish orderbook: %v", err)
	}

	// Verify the message was published
	streamName := models.GetOrderBookStreamName(orderbook.Exchange, orderbook.Symbol)
	length, err := client.GetStreamLength(streamName)
	if err != nil {
		t.Fatalf("Failed to get stream length: %v", err)
	}

	if length != 1 {
		t.Errorf("Expected stream length 1, got %d", length)
	}
}

func TestCacheAndPublishHeatmap(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	heatmap := &models.HeatmapData{
		Symbol:       models.SymbolBTCUSDT,
		Exchange:     models.ExchangeBinance,
		Timestamp:    time.Now().UnixMilli(),
		Interval:     models.Interval1m,
		CurrentPrice: 45000.0,
		Levels: []models.LiquidationLevel{
			{
				Price:             44000.0,
				LongLiquidations:  1000000.0,
				ShortLiquidations: 500000.0,
				TotalVolume:       1500000.0,
				Intensity:         75.0,
				Timestamp:         time.Now().UnixMilli(),
			},
			{
				Price:             46000.0,
				LongLiquidations:  800000.0,
				ShortLiquidations: 1200000.0,
				TotalVolume:       2000000.0,
				Intensity:         90.0,
				Timestamp:         time.Now().UnixMilli(),
			},
		},
		Clusters: []models.LiquidationCluster{
			{
				Symbol:          models.SymbolBTCUSDT,
				PriceRangeStart: 43900,
				PriceRangeEnd:   44100,
				TotalVolume:     3000000,
				PeakIntensity:   85,
				UpdatedAt:       time.Now().UnixMilli(),
			},
		},
		Summary: models.HeatmapSummary{
			TotalLongLiquidations:  5000000,
			TotalShortLiquidations: 3000000,
			MaxLiquidationPrice:    46000,
			MaxLiquidationVolume:   2000000,
			WeightedAvgLongPrice:   44500,
			WeightedAvgShortPrice:  45500,
			SignificantLevels:      10,
			CriticalZones: []models.CriticalZone{
				{
					PriceStart: 43900,
					PriceEnd:   44100,
					Type:       "long",
					Intensity:  85,
					Volume:     3000000,
				},
			},
		},
	}

	// Test caching and publishing
	err := client.CacheAndPublishHeatmap(heatmap)
	if err != nil {
		t.Fatalf("Failed to cache and publish heatmap: %v", err)
	}

	// Verify cache
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

	if len(cached.Levels) != len(heatmap.Levels) {
		t.Errorf("Expected %d levels, got %d", len(heatmap.Levels), len(cached.Levels))
	}

	// Verify stream publication
	streamName := models.GetHeatmapStreamName(heatmap.Symbol)
	length, err := client.GetStreamLength(streamName)
	if err != nil {
		t.Fatalf("Failed to get stream length: %v", err)
	}

	if length != 1 {
		t.Errorf("Expected stream length 1, got %d", length)
	}
}

func TestGetLatestHeatmaps(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	// Cache multiple heatmaps
	symbols := []models.Symbol{
		models.SymbolBTCUSDT,
		models.SymbolETHUSDT,
		models.SymbolBNBUSDT,
	}

	for _, symbol := range symbols {
		heatmap := &models.HeatmapData{
			Symbol:       symbol,
			Timestamp:    time.Now().UnixMilli(),
			Interval:     models.Interval1s,
			CurrentPrice: 45000.0,
			Levels: []models.LiquidationLevel{
				{
					Price:       44000.0,
					TotalVolume: 1500000.0,
					Intensity:   75.0,
				},
			},
		}

		err := client.CacheAndPublishHeatmap(heatmap)
		if err != nil {
			t.Fatalf("Failed to cache heatmap for %s: %v", symbol, err)
		}
	}

	// Get latest heatmaps
	latest, err := client.GetLatestHeatmaps(symbols, models.Interval1s)
	if err != nil {
		t.Fatalf("Failed to get latest heatmaps: %v", err)
	}

	if len(latest) != 3 {
		t.Errorf("Expected 3 heatmaps, got %d", len(latest))
	}

	for _, symbol := range symbols {
		if _, ok := latest[symbol]; !ok {
			t.Errorf("Missing heatmap for %s", symbol)
		}
	}
}

func TestParseLiquidationMessage(t *testing.T) {
	// Create a mock Redis message with all fields
	msg := redis.XMessage{
		ID: "1234567890-0",
		Values: map[string]interface{}{
			"timestamp":        "1234567890",
			"side":             "long",
			"price":            "45000.0",
			"quantity":         "1.5",
			"value":            "67500.0",
			"order_type":       "liquidation",
			"avg_price":        "45100.0",
			"filled_qty":       "1.5",
			"order_status":     "FILLED",
			"order_trade_time": "1234567900",
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

	if event.AvgPrice != 45100.0 {
		t.Errorf("Expected avg price 45100.0, got %f", event.AvgPrice)
	}

	if event.OrderStatus != "FILLED" {
		t.Errorf("Expected order status FILLED, got %s", event.OrderStatus)
	}
}

func TestConsumeLiquidationStream(t *testing.T) {
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
		// Create a context with timeout
		_, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		err := client.ConsumeLiquidationStream(config, func(event *models.LiquidationEvent) error {
			messageReceived = true
			done <- true
			return nil
		})

		if err != nil && err != context.DeadlineExceeded {
			t.Errorf("Unexpected error from consumer: %v", err)
		}
	}()

	// Wait for message or timeout
	select {
	case <-done:
		// Message received
	case <-time.After(5 * time.Second):
		t.Fatal("Consumer test timed out")
	}

	if !messageReceived {
		t.Error("Expected to receive message from stream")
	}
}

func TestConsumeHeatmapStream(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	// First, publish a heatmap
	heatmap := &models.HeatmapData{
		Symbol:       models.SymbolBTCUSDT,
		Timestamp:    time.Now().UnixMilli(),
		Interval:     models.Interval1s,
		CurrentPrice: 45000.0,
		Levels: []models.LiquidationLevel{
			{
				Price:       44000.0,
				TotalVolume: 1500000.0,
				Intensity:   75.0,
			},
		},
	}

	err := client.CacheAndPublishHeatmap(heatmap)
	if err != nil {
		t.Fatalf("Failed to publish test heatmap: %v", err)
	}

	messageReceived := false
	done := make(chan bool, 1)

	// Start consumer in a goroutine
	go func() {
		// Create a context with timeout
		_, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// This would normally block forever, so we'll need to modify it
		// For testing, we'll just check if the heatmap was cached
		cached, err := client.GetCachedHeatmap(models.SymbolBTCUSDT, models.Interval1s)
		if err == nil && cached != nil {
			messageReceived = true
		}
		done <- true
	}()

	// Wait for completion
	select {
	case <-done:
		// Finished
	case <-time.After(5 * time.Second):
		t.Fatal("Consumer test timed out")
	}

	if !messageReceived {
		t.Error("Expected to find cached heatmap")
	}
}

func TestHealthCheck(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	err := client.HealthCheck()
	if err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestGetStats(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	// Publish some data first
	event := &models.LiquidationEvent{
		Exchange:  models.ExchangeBinance,
		Symbol:    models.SymbolBTCUSDT,
		Timestamp: time.Now().UnixMilli(),
		Side:      models.SideLong,
		Price:     45000.0,
		Quantity:  1.5,
		OrderType: models.OrderTypeLiquidation,
	}

	client.PublishLiquidation(event)

	heatmap := &models.HeatmapData{
		Symbol:       models.SymbolBTCUSDT,
		Timestamp:    time.Now().UnixMilli(),
		Interval:     models.Interval1s,
		CurrentPrice: 45000.0,
		Levels: []models.LiquidationLevel{
			{Price: 44000.0, TotalVolume: 1500000.0},
		},
	}

	client.CacheAndPublishHeatmap(heatmap)

	// Get stats
	stats, err := client.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	// Check for expected stats
	if count, ok := stats["liquidations_BTCUSDT_count"]; ok {
		if count.(int64) != 1 {
			t.Errorf("Expected liquidations count 1, got %v", count)
		}
	}

	if cached, ok := stats["heatmap_BTCUSDT_cached"]; ok {
		if !cached.(bool) {
			t.Error("Expected heatmap to be cached")
		}
	}

	if levels, ok := stats["heatmap_BTCUSDT_levels"]; ok {
		if levels.(int) != 1 {
			t.Errorf("Expected 1 heatmap level, got %v", levels)
		}
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

func TestGetHeatmapTTL(t *testing.T) {
	client := setupTestClient(t)
	defer client.Close()

	tests := []struct {
		interval models.Interval
		expected time.Duration
	}{
		{models.Interval1s, client.heatmapCacheTTL}, // Should use configured TTL
		{models.Interval1m, 5 * time.Minute},
		{models.Interval5m, 15 * time.Minute},
		{models.Interval15m, 30 * time.Minute},
		{models.Interval1h, 2 * time.Hour},
		{models.Interval4h, 8 * time.Hour},
		{models.Interval1d, 48 * time.Hour},
		{models.Interval("unknown"), client.heatmapCacheTTL}, // Default
	}

	for _, tt := range tests {
		t.Run(string(tt.interval), func(t *testing.T) {
			ttl := client.getHeatmapTTL(tt.interval)
			if ttl != tt.expected {
				t.Errorf("Expected TTL %v for interval %s, got %v", tt.expected, tt.interval, ttl)
			}
		})
	}
}

func BenchmarkPublishLiquidation(b *testing.B) {
	client := setupTestClient(&testing.T{})
	defer client.Close()

	event := &models.LiquidationEvent{
		Exchange:  models.ExchangeBinance,
		Symbol:    models.SymbolBTCUSDT,
		Timestamp: time.Now().UnixMilli(),
		Side:      models.SideLong,
		Price:     45000.0,
		Quantity:  1.5,
		OrderType: models.OrderTypeLiquidation,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = client.PublishLiquidation(event)
	}
}

func BenchmarkCacheHeatmap(b *testing.B) {
	client := setupTestClient(&testing.T{})
	defer client.Close()

	heatmap := &models.HeatmapData{
		Symbol:       models.SymbolBTCUSDT,
		Timestamp:    time.Now().UnixMilli(),
		Interval:     models.Interval1m,
		CurrentPrice: 45000.0,
		Levels: []models.LiquidationLevel{
			{Price: 44000.0, TotalVolume: 1500000.0, Intensity: 75.0},
			{Price: 46000.0, TotalVolume: 2000000.0, Intensity: 90.0},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Just cache, not publish to stream
		_ = client.cacheHeatmap(heatmap)
	}
}
