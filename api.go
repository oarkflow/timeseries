package timeseries

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Security configuration
type SecurityConfig struct {
	APIKeys            map[string]string // key -> user
	RateLimitRequests  int
	RateLimitWindow    time.Duration
	EnableTLS          bool
	TLSCertFile        string
	TLSKeyFile         string
	CORSAllowedOrigins []string
}

// Rate limiter
type RateLimiter struct {
	mu      sync.RWMutex
	clients map[string][]time.Time
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		clients: make(map[string][]time.Time),
	}
}

func (rl *RateLimiter) Allow(clientIP string, maxRequests int, window time.Duration) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-window)

	// Clean old entries
	if requests, exists := rl.clients[clientIP]; exists {
		validRequests := make([]time.Time, 0, len(requests))
		for _, req := range requests {
			if req.After(windowStart) {
				validRequests = append(validRequests, req)
			}
		}
		rl.clients[clientIP] = validRequests

		if len(validRequests) >= maxRequests {
			return false
		}
	}

	// Add current request
	rl.clients[clientIP] = append(rl.clients[clientIP], now)
	return true
}

// GenerateAPIKey generates a secure API key
func GenerateAPIKey() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

// ValidateAPIKey validates an API key using constant-time comparison
func (sc *SecurityConfig) ValidateAPIKey(apiKey string) (string, bool) {
	if sc.APIKeys == nil {
		return "", false
	}

	for key, user := range sc.APIKeys {
		if subtle.ConstantTimeCompare([]byte(key), []byte(apiKey)) == 1 {
			return user, true
		}
	}
	return "", false
}

// Security middleware
func (api *API) securityMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set security headers
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

		// CORS handling
		if api.security != nil && len(api.security.CORSAllowedOrigins) > 0 {
			origin := r.Header.Get("Origin")
			for _, allowed := range api.security.CORSAllowedOrigins {
				if origin == allowed {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
					break
				}
			}
		}

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Rate limiting
		if api.rateLimiter != nil {
			clientIP := getClientIP(r)
			if !api.rateLimiter.Allow(clientIP, api.security.RateLimitRequests, api.security.RateLimitWindow) {
				api.logAudit("RATE_LIMIT_EXCEEDED", "", clientIP, false)
				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}
		}

		// API Key authentication
		if api.security != nil && len(api.security.APIKeys) > 0 {
			apiKey := r.Header.Get("X-API-Key")
			if apiKey == "" {
				apiKey = r.URL.Query().Get("api_key")
			}

			if apiKey == "" {
				api.logAudit("MISSING_API_KEY", "", getClientIP(r), false)
				http.Error(w, "API key required", http.StatusUnauthorized)
				return
			}

			user, valid := api.security.ValidateAPIKey(apiKey)
			if !valid {
				api.logAudit("INVALID_API_KEY", "", getClientIP(r), false)
				http.Error(w, "Invalid API key", http.StatusUnauthorized)
				return
			}

			// Store user in context for audit logging
			r.Header.Set("X-User", user)
		}

		// Input validation and sanitization
		if err := api.validateAndSanitizeRequest(r); err != nil {
			api.logAudit("INVALID_REQUEST", r.Header.Get("X-User"), getClientIP(r), false)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		next(w, r)
	}
}

// getClientIP extracts the real client IP from headers
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in case of multiple
		if idx := strings.Index(xff, ","); idx > 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}

	// Fall back to RemoteAddr
	if idx := strings.LastIndex(r.RemoteAddr, ":"); idx > 0 {
		return r.RemoteAddr[:idx]
	}
	return r.RemoteAddr
}

// validateAndSanitizeRequest validates and sanitizes request inputs
func (api *API) validateAndSanitizeRequest(r *http.Request) error {
	// Validate query parameters
	for key, values := range r.URL.Query() {
		for _, value := range values {
			if len(value) > 1000 {
				return fmt.Errorf("parameter %s too long", key)
			}
			// Basic sanitization - remove potentially dangerous characters
			if strings.ContainsAny(value, "<>\"'&") {
				return fmt.Errorf("invalid characters in parameter %s", key)
			}
		}
	}

	// Validate headers
	for key, values := range r.Header {
		for _, value := range values {
			if len(value) > 10000 {
				return fmt.Errorf("header %s too long", key)
			}
		}
	}

	return nil
}

// logAudit logs security and access events
func (api *API) logAudit(action, userID, ip string, success bool) {
	entry := AuditEntry{
		Timestamp: time.Now(),
		UserID:    userID,
		Action:    action,
		Resource:  "",
		IP:        ip,
		Success:   success,
	}

	api.auditMu.Lock()
	api.auditLog = append(api.auditLog, entry)
	// Keep only last 1000 entries
	if len(api.auditLog) > 1000 {
		api.auditLog = api.auditLog[len(api.auditLog)-1000:]
	}
	api.auditMu.Unlock()
}

// GetAuditLog returns the audit log
func (api *API) GetAuditLog() []AuditEntry {
	api.auditMu.RLock()
	defer api.auditMu.RUnlock()

	result := make([]AuditEntry, len(api.auditLog))
	copy(result, api.auditLog)
	return result
}

// API represents the HTTP API server
type API struct {
	db           *PersistentDatabase
	server       *http.Server
	security     *SecurityConfig
	rateLimiter  *RateLimiter
	auditLog     []AuditEntry
	auditMu      sync.RWMutex
	reloadTicker *time.Ticker
	reloadDone   chan struct{}
}

// AuditEntry represents an audit log entry
type AuditEntry struct {
	Timestamp time.Time `json:"timestamp"`
	UserID    string    `json:"user_id"`
	Action    string    `json:"action"`
	Resource  string    `json:"resource"`
	IP        string    `json:"ip"`
	Success   bool      `json:"success"`
}

// NewSecureAPI creates a new API server with security features
func NewSecureAPI(db *PersistentDatabase, port int, security *SecurityConfig) *API {
	api := &API{
		db:          db,
		security:    security,
		rateLimiter: NewRateLimiter(),
		auditLog:    make([]AuditEntry, 0, 1000),
		reloadDone:  make(chan struct{}),
	}

	mux := http.NewServeMux()

	// Apply security middleware to all routes
	mux.HandleFunc("/insert", api.securityMiddleware(api.handleInsert))
	mux.HandleFunc("/query", api.securityMiddleware(api.handleQuery))
	mux.HandleFunc("/aggregate", api.securityMiddleware(api.handleAggregate))
	mux.HandleFunc("/increment", api.securityMiddleware(api.handleIncrement))
	mux.HandleFunc("/decrement", api.securityMiddleware(api.handleDecrement))
	mux.HandleFunc("/pattern", api.securityMiddleware(api.handlePatternQuery))
	mux.HandleFunc("/series", api.securityMiddleware(api.handleListSeries))
	mux.HandleFunc("/reload", api.securityMiddleware(api.handleReload))
	mux.HandleFunc("/health", api.securityMiddleware(api.handleHealth))
	mux.HandleFunc("/audit", api.securityMiddleware(api.handleAuditLog))

	api.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	// Configure TLS if enabled
	if security != nil && security.EnableTLS {
		api.server.TLSConfig = nil // Will use default TLS config
	}

	return api
}

// NewAPI creates a new API server (backward compatibility)
func NewAPI(db *PersistentDatabase, port int) *API {
	return NewSecureAPI(db, port, nil)
}

// Start starts the API server
func (api *API) Start() error {
	// Start periodic reload
	api.reloadTicker = time.NewTicker(10 * time.Second) // Reload every 10 seconds
	go func() {
		for {
			select {
			case <-api.reloadTicker.C:
				if err := api.db.Reload(); err != nil {
					log.Printf("Periodic reload error: %v", err)
				} else {
					log.Printf("Periodic reload completed")
				}
			case <-api.reloadDone:
				return
			}
		}
	}()

	if api.security != nil && api.security.EnableTLS {
		return api.server.ListenAndServeTLS(
			api.security.TLSCertFile,
			api.security.TLSKeyFile,
		)
	}
	return api.server.ListenAndServe()
}

// Stop stops the API server
func (api *API) Stop() error {
	if api.reloadTicker != nil {
		api.reloadTicker.Stop()
		close(api.reloadDone)
	}
	return api.server.Close()
}

// handleInsert handles POST /insert
func (api *API) handleInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SeriesName string  `json:"series"`
		Timestamp  int64   `json:"timestamp"`
		Value      float64 `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	ts := time.Unix(req.Timestamp, 0)
	// optional sync flag (force fsync per-operation)
	syncParam := r.URL.Query().Get("sync")
	if syncParam == "true" {
		if err := api.db.InsertSync(req.SeriesName, ts, req.Value); err != nil {
			log.Printf("Insert error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if err := api.db.Insert(req.SeriesName, ts, req.Value); err != nil {
			log.Printf("Insert error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	log.Printf("Inserted point: series=%s, timestamp=%d, value=%f, sync=%s", req.SeriesName, req.Timestamp, req.Value, syncParam)
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
}

// handleQuery handles GET /query?series=name&start=ts&end=ts&downsample=interval
func (api *API) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	seriesName := r.URL.Query().Get("series")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	downsampleStr := r.URL.Query().Get("downsample")

	if seriesName == "" || startStr == "" || endStr == "" {
		http.Error(w, "Missing parameters", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid start timestamp", http.StatusBadRequest)
		return
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid end timestamp", http.StatusBadRequest)
		return
	}

	startTime := time.Unix(start, 0)
	endTime := time.Unix(end, 0)

	// Use pooled query to reduce allocations; caller must release.
	points, err := api.db.QueryPooled(seriesName, startTime, endTime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if points == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]Point{})
		return
	}

	// Apply downsampling if requested (Downsample returns a new slice)
	if downsampleStr != "" {
		interval, err := time.ParseDuration(downsampleStr)
		if err != nil {
			ReleasePointSlice(points)
			http.Error(w, "Invalid downsample interval", http.StatusBadRequest)
			return
		}
		ds := Downsample(points, interval)
		// release pooled points before encoding downsampled result
		ReleasePointSlice(points)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ds)
		return
	}

	// Encode and release pooled slice
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(points)
	ReleasePointSlice(points)
}

// handleAggregate handles GET /aggregate?series=name&start=ts&end=ts
func (api *API) handleAggregate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	seriesName := r.URL.Query().Get("series")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	if seriesName == "" || startStr == "" || endStr == "" {
		http.Error(w, "Missing parameters", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid start timestamp", http.StatusBadRequest)
		return
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid end timestamp", http.StatusBadRequest)
		return
	}

	startTime := time.Unix(start, 0)
	endTime := time.Unix(end, 0)

	agg, err := api.db.QueryAggregate(seriesName, startTime, endTime)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(agg)
}

// handleListSeries handles GET /series
func (api *API) handleListSeries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	series := api.db.ListSeries()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(series)
}

// handleIncrement handles POST /increment
func (api *API) handleIncrement(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SeriesName string `json:"series"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	currentValue, err := api.db.Increment(req.SeriesName)
	if err != nil {
		log.Printf("Increment error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Incremented series: %s to value: %f", req.SeriesName, currentValue)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"value": %f}`, currentValue)
}

// handleDecrement handles POST /decrement
func (api *API) handleDecrement(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SeriesName string `json:"series"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	currentValue, err := api.db.Decrement(req.SeriesName)
	if err != nil {
		log.Printf("Decrement error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Decremented series: %s to value: %f", req.SeriesName, currentValue)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"value": %f}`, currentValue)
}

// handlePatternQuery handles GET /pattern?pattern=user:1:*
func (api *API) handlePatternQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		http.Error(w, "Missing pattern parameter", http.StatusBadRequest)
		return
	}

	includePoints := r.URL.Query().Get("include_points") == "true"
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	var startTime, endTime time.Time
	var parseErr error
	if includePoints {
		if startStr != "" {
			var s int64
			s, parseErr = strconv.ParseInt(startStr, 10, 64)
			if parseErr == nil {
				startTime = time.Unix(s, 0)
			}
		}
		if endStr != "" {
			var e int64
			e, parseErr = strconv.ParseInt(endStr, 10, 64)
			if parseErr == nil {
				endTime = time.Unix(e, 0)
			}
		}
	}

	baseResult, err := api.db.QueryPattern(pattern)
	if err != nil {
		log.Printf("Pattern query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Println(baseResult)

	// baseResult is map[string]float64 of full series names
	if !includePoints {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(baseResult)
		return
	}

	type seriesPoints struct {
		Series string  `json:"series"`
		Points []Point `json:"points"`
	}

	out := make([]seriesPoints, 0, len(baseResult))

	for fullName := range baseResult {
		// determine range
		s := startTime
		e := endTime
		if s.IsZero() && e.IsZero() {
			s = time.Unix(0, 0)
			e = time.Now().Add(100 * 365 * 24 * time.Hour)
		}
		pts, err := api.db.QueryPooled(fullName, s, e)
		if err != nil {
			continue
		}
		if pts == nil {
			out = append(out, seriesPoints{Series: fullName, Points: []Point{}})
			continue
		}
		copied := make([]Point, len(pts))
		copy(copied, pts)
		ReleasePointSlice(pts)
		out = append(out, seriesPoints{Series: fullName, Points: copied})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// handleAuditLog handles GET /audit
func (api *API) handleAuditLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	auditLog := api.GetAuditLog()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(auditLog)
}

// handleReload handles POST /reload
func (api *API) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := api.db.Reload(); err != nil {
		log.Printf("Reload error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Database reloaded from WAL")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
}

// handleHealth handles GET /health
func (api *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
}
