package main

import (
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/timeseries"
)

func main() {

	// Create a persistent database
	db, err := timeseries.NewPersistentDatabase("./data")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Insert some data points
	now := time.Now()
	for i := 0; i < 100; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		err := db.Insert("temperature", ts, float64(20+i))
		if err != nil {
			log.Printf("Insert error: %v", err)
		}
	}

	// Query data
	start := now
	end := now.Add(50 * time.Second)
	points, err := db.Query("temperature", start, end)
	if err != nil {
		log.Printf("Query error: %v", err)
	} else {
		fmt.Printf("Queried %d points\n", len(points))
		for _, p := range points[:5] { // Show first 5
			fmt.Printf("Timestamp: %s, Value: %.2f\n", time.Unix(0, p.Timestamp).Format(time.RFC3339), p.Value)
		}
	}

	// Test increment/decrement
	reqValue1, err := db.Increment("requests")
	if err != nil {
		log.Printf("Increment error: %v", err)
	} else {
		fmt.Printf("Requests after first increment: %.0f\n", reqValue1)
	}
	reqValue2, err := db.Increment("requests")
	if err != nil {
		log.Printf("Increment error: %v", err)
	} else {
		fmt.Printf("Requests after second increment: %.0f\n", reqValue2)
	}
	errValue, err := db.Decrement("errors")
	if err != nil {
		log.Printf("Decrement error: %v", err)
	} else {
		fmt.Printf("Errors after decrement: %.0f\n", errValue)
	}

	// Test pattern matching
	_, err = db.Increment("user:1:total_requests")
	if err != nil {
		log.Printf("Increment error: %v", err)
	}
	_, err = db.Increment("user:1:total_offers")
	if err != nil {
		log.Printf("Increment error: %v", err)
	}
	_, err = db.Increment("user:1:total_offers")
	if err != nil {
		log.Printf("Increment error: %v", err)
	}

	// Create total_stories with initial value 0
	_, err = db.Increment("user:1:total_stories") // starts at 0, increment to 1, then decrement back
	if err != nil {
		log.Printf("Increment error: %v", err)
	}
	_, err = db.Decrement("user:1:total_stories") // back to 0
	if err != nil {
		log.Printf("Decrement error: %v", err)
	}

	// Add data for another user
	_, err = db.Increment("user:2:total_requests")
	if err != nil {
		log.Printf("Increment error: %v", err)
	}
	_, err = db.Increment("user:2:total_requests")
	if err != nil {
		log.Printf("Increment error: %v", err)
	}

	// Query pattern user:1:*
	user1Result, err := db.QueryPattern("user:1:*")
	if err != nil {
		log.Printf("Pattern query error: %v", err)
	} else {
		fmt.Printf("Pattern query user:1:* result: %+v\n", user1Result)
		fmt.Printf("Pattern query user:1:* type: %T\n", user1Result)
	}

	// Query pattern user:*
	allUsersResult, err := db.QueryPattern("user:*")
	if err != nil {
		log.Printf("Pattern query error: %v", err)
	} else {
		fmt.Printf("Pattern query user:* result: %+v\n", allUsersResult)
		fmt.Printf("Pattern query user:* type: %T\n", allUsersResult)
	}

	// Debug: list all series
	series := db.ListSeries()
	fmt.Printf("All series in database: %+v\n", series)

	// Query counters
	reqPoints, err := db.Query("requests", now.Add(-1*time.Hour), now.Add(1*time.Hour))
	if err == nil && len(reqPoints) > 0 {
		fmt.Printf("Requests counter: %.0f\n", reqPoints[len(reqPoints)-1].Value)
	}

	// Get aggregates
	agg, err := db.QueryAggregate("temperature", start, end)
	if err != nil {
		log.Printf("Aggregate error: %v", err)
	} else {
		fmt.Printf("Aggregate - Sum: %.2f, Count: %d, Avg: %.2f, Min: %.2f, Max: %.2f, Median: %.2f, StdDev: %.2f, P95: %.2f, P99: %.2f\n",
			agg.Sum, agg.Count, agg.Avg, agg.Min, agg.Max, agg.Median, agg.StdDev, agg.P95, agg.P99)
	}
}
