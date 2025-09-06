package timeseries

import (
	"encoding/binary"
	"math"
)

// CompressedSeries represents a compressed series
type CompressedSeries struct {
	Name         string
	Timestamps   []int64
	Values       []float64
	Compressed   []byte
	OriginalSize int
}

// CompressSeries compresses a series using delta encoding
func CompressSeries(series *Series) *CompressedSeries {
	series.mu.RLock()
	defer series.mu.RUnlock()

	if len(series.Points) == 0 {
		return &CompressedSeries{Name: series.Name}
	}

	// Delta encoding for timestamps
	deltaTimestamps := make([]int64, len(series.Points))
	deltaTimestamps[0] = series.Points[0].Timestamp
	for i := 1; i < len(series.Points); i++ {
		deltaTimestamps[i] = series.Points[i].Timestamp - series.Points[i-1].Timestamp
	}

	// XOR encoding for values (for floating point, use bit representation)
	xoredValues := make([]uint64, len(series.Points))
	xoredValues[0] = math.Float64bits(series.Points[0].Value)
	for i := 1; i < len(series.Points); i++ {
		xoredValues[i] = math.Float64bits(series.Points[i].Value) ^ math.Float64bits(series.Points[i-1].Value)
	}

	// Serialize to bytes
	buf := make([]byte, 0, len(series.Points)*16)
	for i := 0; i < len(series.Points); i++ {
		tsBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(tsBytes, uint64(deltaTimestamps[i]))
		buf = append(buf, tsBytes...)

		valBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valBytes, xoredValues[i])
		buf = append(buf, valBytes...)
	}

	return &CompressedSeries{
		Name:         series.Name,
		Timestamps:   deltaTimestamps,
		Values:       make([]float64, len(series.Points)), // Decompressed on demand
		Compressed:   buf,
		OriginalSize: len(series.Points) * 16,
	}
}

// DecompressSeries decompresses a compressed series
func (cs *CompressedSeries) DecompressSeries() *Series {
	if len(cs.Compressed) == 0 {
		return NewSeries(cs.Name)
	}

	points := make([]Point, 0, len(cs.Compressed)/16)
	buf := cs.Compressed

	var prevTs int64
	var prevValBits uint64

	for i := 0; i < len(buf); i += 16 {
		deltaTs := int64(binary.LittleEndian.Uint64(buf[i : i+8]))
		xoredVal := binary.LittleEndian.Uint64(buf[i+8 : i+16])

		ts := prevTs + deltaTs
		valBits := prevValBits ^ xoredVal
		val := math.Float64frombits(valBits)

		points = append(points, Point{Timestamp: ts, Value: val})

		prevTs = ts
		prevValBits = valBits
	}

	series := NewSeries(cs.Name)
	series.Points = points
	return series
}

// CompressionRatio returns the compression ratio
func (cs *CompressedSeries) CompressionRatio() float64 {
	if cs.OriginalSize == 0 {
		return 1.0
	}
	return float64(len(cs.Compressed)) / float64(cs.OriginalSize)
}
