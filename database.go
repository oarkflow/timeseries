package timeseries

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// Map pools for reducing allocations
var (
	pointSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Point, 0, 1024)
		},
	}
	float64SlicePool = sync.Pool{
		New: func() interface{} {
			return make([]float64, 0, 1024)
		},
	}
)

// Insert inserts a point into a series
func (db *Database) Insert(seriesName string, timestamp time.Time, value float64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	series, exists := db.series[seriesName]
	if !exists {
		series = NewSeries(seriesName)
		db.series[seriesName] = series
	}

	point := Point{
		Timestamp: timestamp.UnixNano(),
		Value:     value,
	}
	series.InsertPoint(point)
	return nil
}

// BatchInsert inserts multiple points efficiently
func (db *Database) BatchInsert(points []struct {
	SeriesName string
	Timestamp  time.Time
	Value      float64
}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, p := range points {
		series, exists := db.series[p.SeriesName]
		if !exists {
			series = NewSeries(p.SeriesName)
			db.series[p.SeriesName] = series
		}

		point := Point{
			Timestamp: p.Timestamp.UnixNano(),
			Value:     p.Value,
		}
		series.InsertPoint(point)
	}
	return nil
}

// Query retrieves points from a series within a time range efficiently
func (db *Database) Query(seriesName string, start, end time.Time) ([]Point, error) {
	db.mu.RLock()
	series, exists := db.series[seriesName]
	db.mu.RUnlock()
	if !exists {
		return nil, errors.New("series not found")
	}

	startNano, endNano := start.UnixNano(), end.UnixNano()

	series.mu.RLock()
	defer series.mu.RUnlock()

	points := series.Points
	if len(points) == 0 {
		return nil, nil
	}

	// lower bound (>= start)
	i := sort.Search(len(points), func(i int) bool {
		return points[i].Timestamp >= startNano
	})

	// upper bound (> end)
	j := sort.Search(len(points), func(i int) bool {
		return points[i].Timestamp > endNano
	})

	if i >= j {
		return nil, nil
	}

	// Allocate once for result slice
	result := make([]Point, j-i)
	copy(result, points[i:j])
	return result, nil
}

// QueryPooled returns points within [start,end] using a pooled slice to reduce allocations.
// Caller MUST call ReleasePointSlice on the returned slice when finished.
func (db *Database) QueryPooled(seriesName string, start, end time.Time) ([]Point, error) {
	db.mu.RLock()
	series, exists := db.series[seriesName]
	db.mu.RUnlock()
	if !exists {
		return nil, errors.New("series not found")
	}

	startNano, endNano := start.UnixNano(), end.UnixNano()

	series.mu.RLock()
	defer series.mu.RUnlock()

	points := series.Points
	if len(points) == 0 {
		return nil, nil
	}

	i := sort.Search(len(points), func(i int) bool { return points[i].Timestamp >= startNano })
	j := sort.Search(len(points), func(i int) bool { return points[i].Timestamp > endNano })
	if i >= j {
		return nil, nil
	}

	count := j - i
	out := pointSlicePool.Get().([]Point)
	if cap(out) < count {
		out = make([]Point, count)
	} else {
		out = out[:count]
	}
	copy(out, points[i:j])
	return out, nil
}

// QueryAggregate retrieves aggregated data from a series within a time range efficiently
func (db *Database) QueryAggregate(seriesName string, start, end time.Time) (Aggregate, error) {
	db.mu.RLock()
	series, exists := db.series[seriesName]
	db.mu.RUnlock()
	if !exists {
		return Aggregate{}, errors.New("series not found")
	}

	startNano, endNano := start.UnixNano(), end.UnixNano()

	series.mu.RLock()
	defer series.mu.RUnlock()

	points := series.Points
	if len(points) == 0 {
		return Aggregate{}, nil
	}

	// lower bound (>= start)
	i := sort.Search(len(points), func(i int) bool {
		return points[i].Timestamp >= startNano
	})

	// upper bound (> end)
	j := sort.Search(len(points), func(i int) bool {
		return points[i].Timestamp > endNano
	})

	if i >= j {
		return Aggregate{}, nil
	}

	// Aggregate directly without extra allocations
	var sum, min, max float64
	min, max = points[i].Value, points[i].Value
	for _, p := range points[i:j] {
		v := p.Value
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	count := float64(j - i)
	return Aggregate{
		Count: int64(count),
		Sum:   sum,
		Avg:   sum / count,
		Min:   min,
		Max:   max,
	}, nil
}

// ListSeries returns a list of all series names
func (db *Database) ListSeries() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.series))
	for name := range db.series {
		// Filter out series names with non-printable characters (likely corrupted data)
		if isValidSeriesName(name) {
			names = append(names, name)
		}
	}
	// Return a deterministic ordering to avoid surprising callers
	// (map iteration order is intentionally randomized by Go).
	sort.Strings(names)
	return names
}

// isValidSeriesName checks if a series name contains only printable characters and is not empty
func isValidSeriesName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r < 32 || r > 126 { // ASCII printable range
			return false
		}
	}
	return true
}

// DeleteSeries removes a series from the database
func (db *Database) DeleteSeries(seriesName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.series[seriesName]; !exists {
		return errors.New("series not found")
	}
	delete(db.series, seriesName)
	return nil
}

// GetSeriesInfo returns information about a series
func (db *Database) GetSeriesInfo(seriesName string) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	series, exists := db.series[seriesName]
	if !exists {
		return 0, errors.New("series not found")
	}

	series.mu.RLock()
	defer series.mu.RUnlock()
	return len(series.Points), nil
}

// Increment increments the latest value in a series (or creates first point with 1)
func (db *Database) Increment(seriesName string) (float64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	series, exists := db.series[seriesName]
	if !exists {
		series = NewSeries(seriesName)
		db.series[seriesName] = series
	}

	series.mu.Lock()
	defer series.mu.Unlock()

	now := time.Now().UnixNano()
	var currentValue float64
	if len(series.Points) == 0 {
		// First point
		currentValue = 1
		point := Point{Timestamp: now, Value: currentValue}
		series.Points = append(series.Points, point)
	} else {
		// Increment latest value
		series.Points[len(series.Points)-1].Value += 1
		currentValue = series.Points[len(series.Points)-1].Value
	}
	return currentValue, nil
}

// Decrement decrements the latest value in a series (or creates first point with -1)
func (db *Database) Decrement(seriesName string) (float64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	series, exists := db.series[seriesName]
	if !exists {
		series = NewSeries(seriesName)
		db.series[seriesName] = series
	}

	series.mu.Lock()
	defer series.mu.Unlock()

	now := time.Now().UnixNano()
	var currentValue float64
	if len(series.Points) == 0 {
		// First point
		currentValue = -1
		point := Point{Timestamp: now, Value: currentValue}
		series.Points = append(series.Points, point)
	} else {
		// Decrement latest value
		series.Points[len(series.Points)-1].Value -= 1
		currentValue = series.Points[len(series.Points)-1].Value
	}
	return currentValue, nil
}

// RetainData retains data within the specified time window
func (db *Database) RetainData(seriesName string, maxAge time.Duration) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	series, exists := db.series[seriesName]
	if !exists {
		return errors.New("series not found")
	}

	series.mu.Lock()
	defer series.mu.Unlock()

	cutoff := time.Now().Add(-maxAge).UnixNano()
	i := 0
	for i < len(series.Points) && series.Points[i].Timestamp < cutoff {
		i++
	}
	series.Points = series.Points[i:]
	return nil
}

// CleanupOldData cleans up old data for all series based on retention policy
func (db *Database) CleanupOldData(maxAge time.Duration) {
	db.mu.RLock()
	seriesNames := make([]string, 0, len(db.series))
	for name := range db.series {
		seriesNames = append(seriesNames, name)
	}
	db.mu.RUnlock()

	for _, name := range seriesNames {
		db.RetainData(name, maxAge)
	}
}

// QueryPattern returns a map of full series name -> latest value for series
// matching a pattern (supports '*' wildcard). Always returns a map (possibly empty).
func (db *Database) QueryPattern(pattern string) (map[string]float64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := make(map[string]float64)

	// Fast path for exact matches
	if !strings.Contains(pattern, "*") {
		series, exists := db.series[pattern]
		if !exists {
			return result, nil
		}
		series.mu.RLock()
		defer series.mu.RUnlock()
		if len(series.Points) > 0 {
			result[pattern] = series.Points[len(series.Points)-1].Value
		}
		return result, nil
	}

	// General wildcard matching: support simple '*' wildcard(s).
	// Split pattern into parts and check sequentially.
	parts := strings.Split(pattern, "*")

	for seriesName, series := range db.series {
		if matchParts(seriesName, parts) {
			series.mu.RLock()
			if len(series.Points) > 0 {
				result[seriesName] = series.Points[len(series.Points)-1].Value
			} else {
				result[seriesName] = 0
			}
			series.mu.RUnlock()
		}
	}

	return result, nil
}

// matchParts checks whether name matches the sequence of parts (pattern split by '*').
// Parts may include empty strings for leading/trailing '*' and multiple '*' in pattern.
func matchParts(name string, parts []string) bool {
	// special-case: pattern == "*"
	if len(parts) == 1 && parts[0] == "" {
		return true
	}
	idx := 0
	// If pattern doesn't start with '*', the first part must match prefix
	if parts[0] != "" {
		if !strings.HasPrefix(name, parts[0]) {
			return false
		}
		idx = len(parts[0])
	}

	// For middle parts, find each sequentially
	for i := 1; i < len(parts)-1; i++ {
		part := parts[i]
		if part == "" {
			continue
		}
		pos := strings.Index(name[idx:], part)
		if pos == -1 {
			return false
		}
		idx += pos + len(part)
	}

	// If pattern doesn't end with '*', last part must match suffix
	last := parts[len(parts)-1]
	if last != "" {
		return strings.HasSuffix(name, last)
	}
	return true
}

// queryPatternUltraFast uses ultra-optimized pattern matching with minimal allocations
func (db *Database) queryPatternUltraFast(pattern string) (interface{}, error) {
	// Pre-calculate pattern components to avoid repeated string operations
	colonCount := strings.Count(pattern, ":")
	asteriskPos := strings.Index(pattern, "*")

	if asteriskPos == -1 {
		return nil, nil
	}

	// Extract prefix and determine pattern type
	prefix := pattern[:asteriskPos]
	var suffix string
	if asteriskPos < len(pattern)-1 {
		suffix = pattern[asteriskPos+1:]
	}

	// Check pattern type based on structure
	if colonCount == 1 && asteriskPos == len(pattern)-1 {
		// Pattern like "user:*" - single colon, asterisk at end = nested
		return db.queryPatternNested(prefix, suffix)
	} else if colonCount >= 2 && asteriskPos == len(pattern)-1 {
		// Pattern like "user:1:*" - multiple colons, asterisk at end = flat
		return db.queryPatternFlat(prefix, suffix)
	} else {
		// Other patterns
		return db.queryPatternNested(prefix, suffix)
	}
}

// queryPatternFlat returns flat results for patterns like "user:1:*"
func (db *Database) queryPatternFlat(prefix, suffix string) (map[string]float64, error) {
	result := make(map[string]float64)
	prefixLen := len(prefix)

	for seriesName, series := range db.series {
		if len(seriesName) > prefixLen && strings.HasPrefix(seriesName, prefix) {
			remaining := seriesName[prefixLen:]
			if suffix == "" || strings.HasSuffix(remaining, suffix) {
				// Extract key (remove suffix if present)
				key := remaining
				if suffix != "" && strings.HasSuffix(key, suffix) {
					key = key[:len(key)-len(suffix)]
				}

				series.mu.RLock()
				if len(series.Points) > 0 {
					result[key] = series.Points[len(series.Points)-1].Value
				} else {
					result[key] = 0
				}
				series.mu.RUnlock()
			}
		}
	}

	return result, nil
}

// queryPatternNested returns nested results for patterns like "user:*"
func (db *Database) queryPatternNested(prefix, suffix string) (map[string]map[string]float64, error) {
	// Pre-allocate result map with estimated capacity
	result := make(map[string]map[string]float64, 16) // Pre-allocate for 16 groups

	prefixLen := len(prefix)
	prefixBytes := []byte(prefix)

	// Single pass with optimized operations
	for seriesName, series := range db.series {
		// Fast bounds check
		if len(seriesName) <= prefixLen {
			continue
		}

		nameBytes := []byte(seriesName)

		// Manual byte comparison for prefix
		prefixMatch := true
		for i := 0; i < len(prefixBytes); i++ {
			if nameBytes[i] != prefixBytes[i] {
				prefixMatch = false
				break
			}
		}

		if !prefixMatch {
			continue
		}

		remaining := seriesName[prefixLen:]

		// Inline suffix removal
		if suffix != "" {
			suffixLen := len(suffix)
			if len(remaining) >= suffixLen &&
				remaining[len(remaining)-suffixLen:] == suffix {
				remaining = remaining[:len(remaining)-suffixLen]
			} else {
				continue
			}
		}

		// Fast colon search using byte operations
		colonPos := -1
		remainingBytes := []byte(remaining)
		for i, b := range remainingBytes {
			if b == ':' {
				colonPos = i
				break
			}
		}

		if colonPos <= 0 {
			continue
		}

		// Extract keys using byte slicing
		groupKey := remaining[:colonPos]
		metricKey := remaining[colonPos+1:]

		// Single map operation with pre-allocated sub-maps
		groupMap := result[groupKey]
		if groupMap == nil {
			groupMap = make(map[string]float64, 8) // Pre-allocate for 8 metrics
			result[groupKey] = groupMap
		}

		// Minimal locking and value retrieval
		series.mu.RLock()
		val := float64(0)
		if len(series.Points) > 0 {
			val = series.Points[len(series.Points)-1].Value
		}
		series.mu.RUnlock()

		groupMap[metricKey] = val
	}

	return result, nil
}

// ReturnPointSlice returns a point slice to the pool for reuse
func ReturnPointSlice(slice []Point) {
	pointSlicePool.Put(slice[:0])
}
