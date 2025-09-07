package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bohunn/gort-redis-streams-client/redis"
	"github.com/bohunn/gort-trade-model/models"
	redisClient "github.com/redis/go-redis/v9"
)

func main() {
	// Example 1: Create Redis client
	fmt.Println("=== Redis Streams Client Example ===")

	config := redis.Config{
		Host:     "localhost",
		Port:     6379,
		Password: "", // No password for local development
		DB:       0,  // Default database
	}

	client, err := redis.NewStreamClient(config)
	if err != nil {
		log.Fatalf("Failed to create Redis client: %v", err)
	}
	defer client.Close()

	fmt.Println("✅ Connected to Redis")

	// Example 2: Publish liquidation events
	fmt.Println("\n=== Publishing Liquidation Events ===")

	liquidationEvent := &models.LiquidationEvent{
		Exchange:  models.ExchangeBinance,
		Symbol:    models.SymbolBTCUSDT,
		Timestamp: time.Now().UnixMilli(),
		Side:      models.SideLong,
		Price:     45000.0,
		Quantity:  1.5,
		Value:     67500.0,
		OrderType: models.OrderTypeLiquidation,
	}

	err = client.PublishLiquidation(liquidationEvent)
	if err != nil {
		log.Fatalf("Failed to publish liquidation: %v", err)
	}
	fmt.Printf("✅ Published liquidation: $%.2f, %.2f BTC\n", liquidationEvent.Price, liquidationEvent.Quantity)

	// Example 3: Publish market snapshot
	fmt.Println("\n=== Publishing Market Snapshot ===")

	marketSnapshot := &models.MarketSnapshot{
		Exchange:        models.ExchangeBinance,
		Symbol:          models.SymbolBTCUSDT,
		Timestamp:       time.Now().UnixMilli(),
		MarkPrice:       45000.0,
		IndexPrice:      44980.0,
		FundingRate:     0.0001,
		OpenInterest:    1000000000, // $1B
		Volume24h:       500000000,  // $500M
		NextFundingTime: time.Now().Add(4 * time.Hour).UnixMilli(),
	}

	err = client.PublishMarketSnapshot(marketSnapshot)
	if err != nil {
		log.Fatalf("Failed to publish market snapshot: %v", err)
	}
	fmt.Printf("✅ Published market data: Mark Price $%.2f, OI $%.0f\n",
		marketSnapshot.MarkPrice, marketSnapshot.OpenInterest)

	// Example 4: Publish position distribution
	fmt.Println("\n=== Publishing Position Distribution ===")

	positionDist := &models.PositionDistribution{
		Exchange:   models.ExchangeBinance,
		Symbol:     models.SymbolBTCUSDT,
		Timestamp:  time.Now().UnixMilli(),
		PriceLevel: 44000.0,
		LongPositions: models.PositionSummary{
			Count:       150,
			Volume:      5000000.0, // $5M
			AvgLeverage: 12.5,
		},
		ShortPositions: models.PositionSummary{
			Count:       75,
			Volume:      2500000.0, // $2.5M
			AvgLeverage: 8.3,
		},
	}

	err = client.PublishPositionDistribution(positionDist)
	if err != nil {
		log.Fatalf("Failed to publish position distribution: %v", err)
	}
	fmt.Printf("✅ Published positions at $%.0f: %d longs, %d shorts\n",
		positionDist.PriceLevel, positionDist.LongPositions.Count, positionDist.ShortPositions.Count)

	// Example 5: Cache and retrieve heatmap data
	fmt.Println("\n=== Caching Heatmap Data ===")

	heatmapData := &models.HeatmapData{
		Symbol:       models.SymbolBTCUSDT,
		Timestamp:    time.Now().UnixMilli(),
		Interval:     models.Interval5m,
		CurrentPrice: 45000.0,
		LiquidationLevels: []models.LiquidationLevel{
			{
				Price:                       44000.0,
				CumulativeLongLiquidations:  1000000.0,
				CumulativeShortLiquidations: 500000.0,
				EstimatedImpact:             2.5,
				Exchanges:                   []models.Exchange{models.ExchangeBinance, models.ExchangeOKX},
			},
			{
				Price:                       46000.0,
				CumulativeLongLiquidations:  200000.0,
				CumulativeShortLiquidations: 1500000.0,
				EstimatedImpact:             3.2,
				Exchanges:                   []models.Exchange{models.ExchangeBinance},
			},
		},
		Metadata: models.HeatmapMetadata{
			ExchangesCovered: []models.Exchange{models.ExchangeBinance, models.ExchangeOKX},
			DataCompleteness: 95.0,
			CalculationTime:  150,
			LastUpdate:       time.Now().UnixMilli(),
		},
	}

	err = client.CacheHeatmap(heatmapData)
	if err != nil {
		log.Fatalf("Failed to cache heatmap: %v", err)
	}
	fmt.Printf("✅ Cached heatmap for %s (%s interval)\n", heatmapData.Symbol, heatmapData.Interval)

	// Retrieve cached heatmap
	cached, err := client.GetCachedHeatmap(models.SymbolBTCUSDT, models.Interval5m)
	if err != nil {
		log.Fatalf("Failed to get cached heatmap: %v", err)
	}
	if cached != nil {
		fmt.Printf("✅ Retrieved cached heatmap: %d liquidation levels, %.1f%% complete\n",
			len(cached.LiquidationLevels), cached.Metadata.DataCompleteness)
	}

	// Example 6: Monitor stream statistics
	fmt.Println("\n=== Stream Monitoring ===")

	liquidationStream := models.GetLiquidationStreamName(models.ExchangeBinance, models.SymbolBTCUSDT)
	marketStream := models.GetMarketStreamName(models.ExchangeBinance, models.SymbolBTCUSDT)
	positionStream := models.GetPositionStreamName(models.ExchangeBinance, models.SymbolBTCUSDT)

	streams := []string{liquidationStream, marketStream, positionStream}

	for _, streamName := range streams {
		length, err := client.GetStreamLength(streamName)
		if err != nil {
			log.Printf("Warning: couldn't get length for %s: %v", streamName, err)
			continue
		}

		info, err := client.GetStreamInfo(streamName)
		if err != nil {
			log.Printf("Warning: couldn't get info for %s: %v", streamName, err)
			continue
		}

		fmt.Printf("📊 %s: %d messages, last ID: %s\n",
			streamName, length, info.LastGeneratedID)
	}

	// Example 7: Consumer setup (demonstrates the pattern, but doesn't run to completion)
	fmt.Println("\n=== Consumer Setup Example ===")

	consumerConfig := redis.ConsumerConfig{
		Group:     "aggregator-group",
		Consumer:  "aggregator-1",
		Streams:   []string{liquidationStream},
		BatchSize: 10,
		BlockMs:   5000, // 5 second timeout
	}

	messageCount := 0
	maxMessages := 3 // Limit for example

	fmt.Printf("🔄 Starting consumer (will process max %d messages)...\n", maxMessages)

	// Create a context with timeout for the example
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		err := client.ConsumeStreams(consumerConfig, func(stream string, msg redisClient.XMessage) error {
			messageCount++

			// Parse the liquidation message
			if stream == liquidationStream {
				event, err := redis.ParseLiquidationMessage(msg)
				if err != nil {
					return fmt.Errorf("failed to parse message: %w", err)
				}

				fmt.Printf("📨 Received liquidation: %s %s $%.2f (%.2f BTC)\n",
					event.Side, event.Symbol, event.Price, event.Quantity)
			}

			// Stop after processing a few messages for example
			if messageCount >= maxMessages {
				return fmt.Errorf("example complete")
			}

			return nil
		})

		if err != nil && messageCount >= maxMessages {
			fmt.Printf("✅ Consumer example completed (%d messages processed)\n", messageCount)
		} else if err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Wait for consumer to process or timeout
	<-ctx.Done()

	// Example 8: Cleanup operations
	fmt.Println("\n=== Cleanup Operations ===")

	// Trim streams to reasonable sizes
	err = client.TrimStream(liquidationStream, 1000)
	if err != nil {
		log.Printf("Warning: failed to trim %s: %v", liquidationStream, err)
	} else {
		fmt.Printf("✅ Trimmed %s to max 1000 messages\n", liquidationStream)
	}

	fmt.Println("\n🎉 Redis Streams Client example completed successfully!")
	fmt.Println("\n💡 Next steps:")
	fmt.Println("   1. Run setup.sh to initialize Redis streams")
	fmt.Println("   2. Start your exchange connectors")
	fmt.Println("   3. Start your aggregation services")
	fmt.Println("   4. Monitor with: redis-cli XINFO STREAM <stream-name>")
}
