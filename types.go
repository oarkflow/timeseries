// package timeseries
// Optimized time-series package: minimal allocations, pools, zero-copy read views when safe,
// corrected percentile/median logic, concurrent-safe series and database operations.
//
// Notes:
//   - To truly avoid allocations on inserts, call s.EnsureCapacity(expectedTotalPoints) once
//     before heavy inserts so append never reallocates.
//   - When using GetPointsCopy, the returned slice was allocated from a pool. When you're done
//     with it, call ReleasePointSlice(result) to return it to the pool.
//   - For aggregated queries with zero-copy, use Series.ComputeAggregate (no slice allocation).
//   - For percentile estimation on very large ranges we sample up to maxSampleSize; sampling logic
//     uses the actual sample length to compute median/percentiles correctly.
//
// Exposed helpers:
//   - NewDatabase, NewSeries, (*Database).InsertPoint, (*Database).GetPointsCopy
//   - (*Series).InsertPoint, (*Series).EnsureCapacity, (*Series).GetSliceView (zero-copy, read-only view)
//   - GetPointsCopy/ReleasePointSlice, ReleaseFloat64Slice, ComputeAggregate
package timeseries

import (
	"math"
	"sort"
	"sync"
	"time"
)

// -------------------------------
// Types
// -------------------------------

// Point represents a single data point in a time series
type Point struct {
	Timestamp int64 // Unix nanoseconds
	Value     float64
}

// Aggregate represents an aggregation result
type Aggregate struct {
	Sum    float64
	Count  int64
	Avg    float64
	Min    float64
	Max    float64
	Median float64
	StdDev float64
	P95    float64
	P99    float64
}

// Series represents a time series with multiple points
type Series struct {
	Name   string
	Points []Point
	mu     sync.RWMutex
}

// Database holds all time series
type Database struct {
	series map[string]*Series
	mu     sync.RWMutex
}

// TimeRange helpers
type TimeRange struct {
	Start time.Time
	End   time.Time
}

func (tr TimeRange) ToUnixNano() (int64, int64) {
	return tr.Start.UnixNano(), tr.End.UnixNano()
}

// Release helpers: callers MUST call these if they received a pooled slice from GetPointsCopy
func ReleasePointSlice(p []Point) {
	// reset len to 0 but keep capacity for reuse
	pointSlicePool.Put(p[:0])
}

func ReleaseFloat64Slice(f []float64) {
	float64SlicePool.Put(f[:0])
}

// -------------------------------
// Constructors
// -------------------------------

func NewDatabase() *Database {
	return &Database{
		series: make(map[string]*Series),
	}
}

func NewSeries(name string) *Series {
	return &Series{
		Name:   name,
		Points: make([]Point, 0, 0),
	}
}

// -------------------------------
// Database methods
// -------------------------------

// GetOrCreateSeries returns the series pointer; creates it if missing.
func (db *Database) GetOrCreateSeries(name string) *Series {
	db.mu.RLock()
	s, ok := db.series[name]
	db.mu.RUnlock()
	if ok {
		return s
	}

	// create
	db.mu.Lock()
	defer db.mu.Unlock()
	// re-check
	if s, ok = db.series[name]; ok {
		return s
	}
	s = NewSeries(name)
	db.series[name] = s
	return s
}

// InsertPoint inserts into a named series (creates series if missing).
// This delegates to Series.InsertPoint.
func (db *Database) InsertPoint(seriesName string, p Point) {
	s := db.GetOrCreateSeries(seriesName)
	s.InsertPoint(p)
}

// GetPointsCopy returns a copy of the points in the range [start,end] using a pooled slice.
// Caller MUST call ReleasePointSlice on the returned slice when done.
func (db *Database) GetPointsCopy(seriesName string, start, end int64) []Point {
	db.mu.RLock()
	s, ok := db.series[seriesName]
	db.mu.RUnlock()
	if !ok {
		return nil
	}
	return s.GetPointsCopy(start, end)
}

// GetSeriesView returns a zero-copy view (slice header referencing internal array).
// WARNING: This is a read-only view. The caller MUST NOT mutate the returned slice or expect it
// to remain valid if the series may be mutated concurrently. Use ComputeAggregate or GetPointsCopy
// if you need a stable copy.
func (db *Database) GetSeriesView(seriesName string) []Point {
	db.mu.RLock()
	s, ok := db.series[seriesName]
	db.mu.RUnlock()
	if !ok {
		return nil
	}
	return s.GetSliceView()
}

// -------------------------------
// Series methods
// -------------------------------

// EnsureCapacity makes sure the underlying slice has capacity >= n.
// Use this BEFORE heavy inserts to avoid reallocations while inserting.
func (s *Series) EnsureCapacity(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cap(s.Points) >= n {
		return
	}
	newCap := cap(s.Points)
	if newCap == 0 {
		newCap = 1
	}
	for newCap < n {
		newCap *= 2
	}
	newSlice := make([]Point, len(s.Points), newCap)
	copy(newSlice, s.Points)
	s.Points = newSlice
}

// InsertPoint adds a point to the series, maintaining sorted order.
// To avoid allocations during insertion, call EnsureCapacity beforehand.
func (s *Series) InsertPoint(p Point) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Quick append if newest
	if len(s.Points) == 0 || s.Points[len(s.Points)-1].Timestamp <= p.Timestamp {
		s.Points = append(s.Points, p)
		return
	}

	// Binary search index to insert at
	i := sort.Search(len(s.Points), func(i int) bool {
		return s.Points[i].Timestamp >= p.Timestamp
	})

	// Insert at i without extra allocations if capacity allows
	if len(s.Points) < cap(s.Points) {
		// grow length by 1
		s.Points = s.Points[:len(s.Points)+1]
		// shift tail right by 1
		copy(s.Points[i+1:], s.Points[i:len(s.Points)-1])
		s.Points[i] = p
		return
	}

	// fallback: capacity insufficient -> allocate new underlying (single allocation)
	newCap := cap(s.Points) * 2
	if newCap == 0 {
		newCap = 1
	}
	if newCap < len(s.Points)+1 {
		newCap = len(s.Points) + 1
	}
	newSlice := make([]Point, len(s.Points)+1, newCap)
	copy(newSlice[:i], s.Points[:i])
	newSlice[i] = p
	copy(newSlice[i+1:], s.Points[i:])
	s.Points = newSlice
}

// GetSliceView returns a read-only view of the entire series points (zero-copy).
// This returns a slice header pointing to the internal array. Do NOT mutate.
func (s *Series) GetSliceView() []Point {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Points[:] // copy header only (zero-copy)
}

// GetPointsCopy returns points within [start,end] as a slice from the pool (copied).
// Caller MUST call ReleasePointSlice when finished.
func (s *Series) GetPointsCopy(start, end int64) []Point {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.Points) == 0 {
		return nil
	}

	// find first index with Timestamp >= start
	startIdx := sort.Search(len(s.Points), func(i int) bool {
		return s.Points[i].Timestamp >= start
	})

	// find first index with Timestamp > end, then subtract 1
	endIdx := sort.Search(len(s.Points), func(i int) bool {
		return s.Points[i].Timestamp > end
	}) - 1

	if startIdx <= endIdx && startIdx < len(s.Points) && endIdx >= 0 {
		count := endIdx - startIdx + 1
		out := pointSlicePool.Get().([]Point)
		if cap(out) < count {
			out = make([]Point, count)
		} else {
			out = out[:count]
		}
		copy(out, s.Points[startIdx:endIdx+1])
		return out
	}
	return nil
}

// ComputeAggregate computes aggregates for points within a time range without copying
// (no allocation except minimal temporaries from pools for percentile calculation).
// This is the recommended path for high-performance aggregation.
func (s *Series) ComputeAggregate(start, end int64) Aggregate {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.Points) == 0 {
		return Aggregate{}
	}

	// find startIdx and endIdx using sort.Search
	startIdx := sort.Search(len(s.Points), func(i int) bool {
		return s.Points[i].Timestamp >= start
	})

	endIdx := sort.Search(len(s.Points), func(i int) bool {
		return s.Points[i].Timestamp > end
	}) - 1

	if startIdx < 0 || startIdx >= len(s.Points) || endIdx < 0 || endIdx < startIdx {
		return Aggregate{}
	}

	count := endIdx - startIdx + 1
	if count <= 0 {
		return Aggregate{}
	}

	// compute sum/min/max/avg/stddev without extra allocation
	sum := 0.0
	min := s.Points[startIdx].Value
	max := s.Points[startIdx].Value

	for i := startIdx; i <= endIdx; i++ {
		v := s.Points[i].Value
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	avg := sum / float64(count)

	// stddev
	var variance float64
	for i := startIdx; i <= endIdx; i++ {
		diff := s.Points[i].Value - avg
		variance += diff * diff
	}
	variance /= float64(count)
	stddev := math.Sqrt(variance)

	// Percentiles/median: for small ranges, sort exact values; for large ranges sample up to maxSampleSize.
	const maxSampleSize = 10000
	var values []float64
	nValues := count

	if count <= maxSampleSize {
		// get pooled slice sized to exact count
		values = float64SlicePool.Get().([]float64)
		if cap(values) < count {
			values = make([]float64, count)
		} else {
			values = values[:count]
		}
		for i := 0; i < count; i++ {
			values[i] = s.Points[startIdx+i].Value
		}
		nValues = count
		sort.Float64s(values[:nValues])
	} else {
		// sample
		sampleSize := maxSampleSize
		step := count / sampleSize
		if step < 1 {
			step = 1
		}

		values = float64SlicePool.Get().([]float64)
		if cap(values) < sampleSize {
			values = make([]float64, sampleSize)
		} else {
			values = values[:sampleSize]
		}

		actual := 0
		for i := 0; i < sampleSize && startIdx+i*step <= endIdx; i++ {
			values[i] = s.Points[startIdx+i*step].Value
			actual++
		}
		nValues = actual
		if nValues == 0 {
			// fallback
			nValues = 1
			values[0] = avg
		}
		sort.Float64s(values[:nValues])
	}

	// median and percentiles computed against actual nValues
	var median float64
	if nValues == 0 {
		median = avg
	} else if nValues%2 == 1 {
		median = values[nValues/2]
	} else {
		median = (values[nValues/2-1] + values[nValues/2]) / 2
	}

	p95Index := int(math.Floor(float64(nValues-1) * 0.95))
	p99Index := int(math.Floor(float64(nValues-1) * 0.99))
	if p95Index < 0 {
		p95Index = 0
	}
	if p99Index < 0 {
		p99Index = 0
	}
	if p95Index >= nValues {
		p95Index = nValues - 1
	}
	if p99Index >= nValues {
		p99Index = nValues - 1
	}
	p95 := values[p95Index]
	p99 := values[p99Index]

	// return value slice to pool
	ReleaseFloat64Slice(values[:nValues])

	return Aggregate{
		Sum:    sum,
		Count:  int64(count),
		Avg:    avg,
		Min:    min,
		Max:    max,
		Median: median,
		StdDev: stddev,
		P95:    p95,
		P99:    p99,
	}
}

// -------------------------------
// Utility aggregate for arbitrary point slice (copy-based)
// -------------------------------

// ComputeAggregateFromSlice computes aggregates for an arbitrary slice of points.
// This function allocates for sorting (a float64 slice) but is useful when you already have a slice.
func ComputeAggregateFromSlice(points []Point) Aggregate {
	if len(points) == 0 {
		return Aggregate{}
	}

	count := len(points)
	sum := 0.0
	min := points[0].Value
	max := points[0].Value

	for i := 0; i < count; i++ {
		v := points[i].Value
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	avg := sum / float64(count)

	values := make([]float64, count)
	for i := 0; i < count; i++ {
		values[i] = points[i].Value
	}
	sort.Float64s(values)

	var median float64
	if count%2 == 1 {
		median = values[count/2]
	} else {
		median = (values[count/2-1] + values[count/2]) / 2
	}

	// stddev from original data
	var variance float64
	for i := 0; i < count; i++ {
		diff := points[i].Value - avg
		variance += diff * diff
	}
	variance /= float64(count)
	stddev := math.Sqrt(variance)

	p95Index := int(math.Floor(float64(count-1) * 0.95))
	p99Index := int(math.Floor(float64(count-1) * 0.99))
	if p95Index < 0 {
		p95Index = 0
	}
	if p99Index < 0 {
		p99Index = 0
	}
	if p95Index >= count {
		p95Index = count - 1
	}
	if p99Index >= count {
		p99Index = count - 1
	}

	return Aggregate{
		Sum:    sum,
		Count:  int64(count),
		Avg:    avg,
		Min:    min,
		Max:    max,
		Median: median,
		StdDev: stddev,
		P95:    values[p95Index],
		P99:    values[p99Index],
	}
}

// -------------------------------
// Downsample
// -------------------------------

// Downsample downsamples points to reduce data points using simple average per bucket.
// interval must be > 0.
func Downsample(points []Point, interval time.Duration) []Point {
	if len(points) == 0 {
		return points
	}
	if interval <= 0 {
		// invalid interval: return copy to avoid modifying input
		out := make([]Point, len(points))
		copy(out, points)
		return out
	}

	// Use bucket index (timestamp / interval)
	nsInterval := int64(interval)
	if nsInterval == 0 {
		out := make([]Point, len(points))
		copy(out, points)
		return out
	}

	out := make([]Point, 0, len(points)/1) // approximate
	currentBucket := points[0].Timestamp / nsInterval
	sum := points[0].Value
	count := 1

	for i := 1; i < len(points); i++ {
		bucket := points[i].Timestamp / nsInterval
		if bucket == currentBucket {
			sum += points[i].Value
			count++
		} else {
			avgTs := currentBucket*nsInterval + nsInterval/2
			avgVal := sum / float64(count)
			out = append(out, Point{Timestamp: avgTs, Value: avgVal})
			currentBucket = bucket
			sum = points[i].Value
			count = 1
		}
	}
	// last
	avgTs := currentBucket*nsInterval + nsInterval/2
	avgVal := sum / float64(count)
	out = append(out, Point{Timestamp: avgTs, Value: avgVal})
	return out
}
