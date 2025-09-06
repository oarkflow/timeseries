package timeseries

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Benchmark data setup
func setupBenchmarkData(b *testing.B, seriesCount, pointsPerSeries int) *Database {
	db := NewDatabase()
	now := time.Now()

	for s := 0; s < seriesCount; s++ {
		seriesName := fmt.Sprintf("bench:series:%d", s)
		for p := 0; p < pointsPerSeries; p++ {
			ts := now.Add(time.Duration(p) * time.Second)
			value := float64(rand.Intn(1000))
			db.Insert(seriesName, ts, value)
		}
	}

	return db
}

// Benchmark Insert operations
func BenchmarkInsert(b *testing.B) {
	db := NewDatabase()
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ts := now.Add(time.Duration(i) * time.Millisecond)
		db.Insert("bench:insert", ts, float64(i))
	}
}

func BenchmarkInsertLarge(b *testing.B) {
	db := NewDatabase()
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ts := now.Add(time.Duration(i) * time.Millisecond)
		db.Insert(fmt.Sprintf("bench:large:%d", i%100), ts, float64(i))
	}
}

// Benchmark Query operations
func BenchmarkQuery(b *testing.B) {
	db := setupBenchmarkData(b, 10, 1000)
	now := time.Now()
	start := now
	end := now.Add(500 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		seriesName := fmt.Sprintf("bench:series:%d", i%10)
		db.Query(seriesName, start, end)
	}
}

func BenchmarkQueryRange(b *testing.B) {
	db := setupBenchmarkData(b, 1, 10000)
	now := time.Now()
	start := now.Add(1000 * time.Second)
	end := now.Add(9000 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db.Query("bench:series:0", start, end)
	}
}

// Benchmark Aggregate operations
func BenchmarkAggregate(b *testing.B) {
	db := setupBenchmarkData(b, 10, 1000)
	now := time.Now()
	start := now
	end := now.Add(500 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		seriesName := fmt.Sprintf("bench:series:%d", i%10)
		db.QueryAggregate(seriesName, start, end)
	}
}

func BenchmarkAggregateLarge(b *testing.B) {
	db := setupBenchmarkData(b, 1, 10000)
	now := time.Now()
	start := now
	end := now.Add(5000 * time.Second)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db.QueryAggregate("bench:series:0", start, end)
	}
}

// Benchmark Increment/Decrement operations
func BenchmarkIncrement(b *testing.B) {
	db := NewDatabase()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		seriesName := fmt.Sprintf("bench:counter:%d", i%100)
		db.Increment(seriesName)
	}
}

func BenchmarkDecrement(b *testing.B) {
	db := NewDatabase()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		seriesName := fmt.Sprintf("bench:counter:%d", i%100)
		db.Decrement(seriesName)
	}
}

// Benchmark Pattern matching
func BenchmarkPatternQuery(b *testing.B) {
	db := NewDatabase()

	// Setup pattern data
	for u := 0; u < 10; u++ {
		for m := 0; m < 5; m++ {
			seriesName := fmt.Sprintf("user:%d:metric%d", u, m)
			db.Increment(seriesName)
			db.Increment(seriesName)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		userID := i % 10
		pattern := fmt.Sprintf("user:%d:*", userID)
		db.QueryPattern(pattern)
	}
}

func BenchmarkPatternQueryNested(b *testing.B) {
	db := NewDatabase()

	// Setup nested pattern data
	for u := 0; u < 10; u++ {
		for m := 0; m < 5; m++ {
			seriesName := fmt.Sprintf("user:%d:metric%d", u, m)
			db.Increment(seriesName)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db.QueryPattern("user:*")
	}
}

// Benchmark Concurrent operations
func BenchmarkConcurrentInsert(b *testing.B) {
	db := NewDatabase()
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		localCounter := 0
		for pb.Next() {
			ts := now.Add(time.Duration(localCounter) * time.Millisecond)
			db.Insert(fmt.Sprintf("bench:concurrent:%d", localCounter%10), ts, float64(localCounter))
			localCounter++
		}
	})
}

func BenchmarkConcurrentIncrement(b *testing.B) {
	db := NewDatabase()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		localCounter := 0
		for pb.Next() {
			seriesName := fmt.Sprintf("bench:concurrent:counter:%d", localCounter%50)
			db.Increment(seriesName)
			localCounter++
		}
	})
}

// Memory benchmark
func BenchmarkMemoryUsage(b *testing.B) {
	db := NewDatabase()
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ts := now.Add(time.Duration(i) * time.Millisecond)
		db.Insert("bench:memory", ts, float64(i))
	}
}

// Batch operations benchmark
func BenchmarkBatchInsert(b *testing.B) {
	db := NewDatabase()
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	batchSize := 100
	for i := 0; i < b.N; i += batchSize {
		points := make([]struct {
			SeriesName string
			Timestamp  time.Time
			Value      float64
		}, batchSize)

		for j := 0; j < batchSize; j++ {
			points[j] = struct {
				SeriesName string
				Timestamp  time.Time
				Value      float64
			}{
				SeriesName: "bench:batch",
				Timestamp:  now.Add(time.Duration(i+j) * time.Millisecond),
				Value:      float64(i + j),
			}
		}

		db.BatchInsert(points)
	}
}

// Performance test with real-world scenario
func BenchmarkRealWorldScenario(b *testing.B) {
	db := NewDatabase()
	now := time.Now()

	// Setup initial data
	for i := 0; i < 1000; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		db.Insert("temperature", ts, 20.0+float64(i%10))
		db.Insert("humidity", ts, 50.0+float64(i%20))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			// Mix of operations
			switch counter % 4 {
			case 0:
				// Insert
				ts := now.Add(time.Duration(counter) * time.Millisecond)
				db.Insert("temperature", ts, float64(counter%100))
			case 1:
				// Query
				start := now.Add(time.Duration(counter%100) * time.Second)
				end := start.Add(10 * time.Second)
				db.Query("temperature", start, end)
			case 2:
				// Aggregate
				start := now.Add(time.Duration(counter%100) * time.Second)
				end := start.Add(10 * time.Second)
				db.QueryAggregate("temperature", start, end)
			case 3:
				// Increment
				db.Increment(fmt.Sprintf("counter:%d", counter%10))
			}
			counter++
		}
	})
}

// Helper function to run benchmarks and collect results
func runBenchmarks() {
	fmt.Println("=== Timeseries Database Benchmark Results ===")
	fmt.Println()

	// Run individual benchmarks
	benchmarks := []struct {
		name string
		fn   func(*testing.B)
	}{
		{"Insert", BenchmarkInsert},
		{"InsertLarge", BenchmarkInsertLarge},
		{"Query", BenchmarkQuery},
		{"QueryRange", BenchmarkQueryRange},
		{"Aggregate", BenchmarkAggregate},
		{"AggregateLarge", BenchmarkAggregateLarge},
		{"Increment", BenchmarkIncrement},
		{"Decrement", BenchmarkDecrement},
		{"PatternQuery", BenchmarkPatternQuery},
		{"PatternQueryNested", BenchmarkPatternQueryNested},
		{"ConcurrentInsert", BenchmarkConcurrentInsert},
		{"ConcurrentIncrement", BenchmarkConcurrentIncrement},
		{"BatchInsert", BenchmarkBatchInsert},
		{"RealWorldScenario", BenchmarkRealWorldScenario},
	}

	for _, bench := range benchmarks {
		fmt.Printf("Running %s...\n", bench.name)
		result := testing.Benchmark(bench.fn)
		fmt.Printf("  %s: %s\n", bench.name, result.String())
		fmt.Printf("  Operations/sec: %.0f\n", float64(result.N)/result.T.Seconds())
		fmt.Printf("  Time/op: %v\n", result.T)
		fmt.Printf("  Allocs/op: %d\n", result.AllocsPerOp())
		fmt.Printf("  Bytes/op: %d\n", result.AllocedBytesPerOp())
		fmt.Println()
	}
}
