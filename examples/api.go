package main

import (
	"log"
	"time"

	"github.com/oarkflow/timeseries"
)

func main() {
	// Create security configuration
	security := &timeseries.SecurityConfig{
		APIKeys: map[string]string{
			"demo-api-key-12345": "demo-user",
		},
		RateLimitRequests:  1000,
		RateLimitWindow:    time.Minute,
		EnableTLS:          false, // Set to true in production
		CORSAllowedOrigins: []string{"http://localhost:3000"},
	}

	// Create a persistent database
	db, err := timeseries.NewPersistentDatabase("./data")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Debug: List series after loading
	series := db.ListSeries()
	log.Printf("Loaded %d series from database: %v", len(series), series)

	// Create secure API server
	api := timeseries.NewSecureAPI(db, 8081, security)
	log.Println("Starting secure API server on :8081")
	if err := api.Start(); err != nil {
		log.Printf("API server error: %v", err)
	}
	api.Stop()
}
