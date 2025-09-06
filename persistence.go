package timeseries

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024)
	},
}

// PersistentDatabase extends Database with persistence
type PersistentDatabase struct {
	*Database
	dataDir   string
	walFile   *os.File
	writeChan chan []byte
	done      chan struct{}
}

// WAL writer tuning (exported so benchmarks can tune at runtime)
var (
	WalFlushBatchSize = 64
	WalFlushInterval  = 50 * time.Millisecond
)

// NewPersistentDatabase creates a new persistent database
func NewPersistentDatabase(dataDir string) (*PersistentDatabase, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	db := NewDatabase()
	pdb := &PersistentDatabase{
		Database:  db,
		dataDir:   dataDir,
		writeChan: make(chan []byte, 1000), // Buffered channel for high throughput
		done:      make(chan struct{}),
	}

	// Open WAL file
	walPath := filepath.Join(dataDir, "wal.log")
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	pdb.walFile = walFile
	fmt.Printf("Opened WAL file: %s\n", walPath)

	// Start async writer
	go pdb.asyncWriter()

	// Load existing data
	if err := pdb.loadFromDisk(); err != nil {
		return nil, err
	}

	return pdb, nil
}

// Insert persists the point
func (pdb *PersistentDatabase) Insert(seriesName string, timestamp time.Time, value float64) error {
	// First, insert in memory
	if err := pdb.Database.Insert(seriesName, timestamp, value); err != nil {
		return err
	}

	// Then, write to WAL
	return pdb.writeToWAL(seriesName, timestamp, value, true)
}

// Increment persists the increment operation
func (pdb *PersistentDatabase) Increment(seriesName string) (float64, error) {
	fmt.Printf("Increment: series=%s\n", seriesName)
	// First, increment in memory
	currentValue, err := pdb.Database.Increment(seriesName)
	if err != nil {
		return 0, err
	}

	// Then, write the updated latest point to WAL
	series := pdb.series[seriesName]
	series.mu.RLock()
	lastPoint := series.Points[len(series.Points)-1]
	series.mu.RUnlock()

	fmt.Printf("Increment: writing to WAL series=%s, ts=%d, value=%f\n", seriesName, lastPoint.Timestamp, lastPoint.Value)
	err = pdb.writeToWAL(seriesName, time.Unix(0, lastPoint.Timestamp), lastPoint.Value, true)
	return currentValue, err
}

// Decrement persists the decrement operation
func (pdb *PersistentDatabase) Decrement(seriesName string) (float64, error) {
	// First, decrement in memory
	currentValue, err := pdb.Database.Decrement(seriesName)
	if err != nil {
		return 0, err
	}

	// Then, write the updated latest point to WAL
	series := pdb.series[seriesName]
	series.mu.RLock()
	lastPoint := series.Points[len(series.Points)-1]
	series.mu.RUnlock()

	err = pdb.writeToWAL(seriesName, time.Unix(0, lastPoint.Timestamp), lastPoint.Value, true)
	return currentValue, err
}

// InsertSync inserts and forces a synchronous WAL write+sync for durability-critical points.
func (pdb *PersistentDatabase) InsertSync(seriesName string, timestamp time.Time, value float64) error {
	if err := pdb.Database.Insert(seriesName, timestamp, value); err != nil {
		return err
	}
	return pdb.writeToWAL(seriesName, timestamp, value, true)
}

// writeToWAL writes the insertion to the WAL. If syncNow is true the write is
// performed synchronously (file write + Sync) to guarantee durability.
func (pdb *PersistentDatabase) writeToWAL(seriesName string, timestamp time.Time, value float64, syncNow bool) error {
	fmt.Printf("writeToWAL: series=%s, ts=%d, value=%f, sync=%v\n", seriesName, timestamp.UnixNano(), value, syncNow)
	// Format: seriesNameLen (4 bytes) | seriesName | timestamp (8 bytes) | value (8 bytes)
	seriesNameBytes := []byte(seriesName)
	seriesNameLen := uint32(len(seriesNameBytes))

	buf := bufferPool.Get().([]byte)
	if cap(buf) < 4+len(seriesNameBytes)+16 {
		buf = make([]byte, 4+len(seriesNameBytes)+16)
	} else {
		buf = buf[:4+len(seriesNameBytes)+16]
	}

	binary.LittleEndian.PutUint32(buf[0:4], seriesNameLen)
	copy(buf[4:4+len(seriesNameBytes)], seriesNameBytes)
	binary.LittleEndian.PutUint64(buf[4+len(seriesNameBytes):4+len(seriesNameBytes)+8], uint64(timestamp.UnixNano()))
	// encode float64 as uint64 bits
	binary.LittleEndian.PutUint64(buf[4+len(seriesNameBytes)+8:4+len(seriesNameBytes)+16], math.Float64bits(value))

	if syncNow {
		// perform synchronous write and sync immediately
		n, err := pdb.walFile.Write(buf)
		if err != nil {
			fmt.Printf("writeToWAL: write error: %v\n", err)
			bufferPool.Put(buf[:0])
			return err
		}
		fmt.Printf("writeToWAL: wrote %d bytes\n", n)
		if err := pdb.walFile.Sync(); err != nil {
			fmt.Printf("writeToWAL: sync error: %v\n", err)
			bufferPool.Put(buf[:0])
			return err
		}
		bufferPool.Put(buf[:0])
		return nil
	}

	// async path: send to async writer (block if channel is full to preserve durability)
	pdb.writeChan <- buf
	return nil
}

// loadFromDisk loads data from WAL on startup
func (pdb *PersistentDatabase) loadFromDisk() error {
	walPath := filepath.Join(pdb.dataDir, "wal.log")
	file, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("No WAL file found, starting with empty database")
			return nil // No WAL yet
		}
		return err
	}
	defer file.Close()
	fmt.Printf("Loading data from WAL: %s\n", walPath)

	buf := make([]byte, 1024)
	offset := int64(0)
	loadedPoints := 0

	for {
		n, err := file.ReadAt(buf, offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		pos := 0
		for pos < n {
			if pos+4 > n {
				break
			}
			seriesNameLen := binary.LittleEndian.Uint32(buf[pos : pos+4])
			pos += 4

			if pos+int(seriesNameLen) > n {
				break
			}
			seriesName := string(buf[pos : pos+int(seriesNameLen)])
			pos += int(seriesNameLen)

			if pos+16 > n {
				break
			}
			timestamp := int64(binary.LittleEndian.Uint64(buf[pos : pos+8]))
			// decode float64 from bits
			valueBits := binary.LittleEndian.Uint64(buf[pos+8 : pos+16])
			value := math.Float64frombits(valueBits)
			pos += 16

			// Skip invalid series names
			if !isValidSeriesName(seriesName) {
				continue
			}
			// Insert into memory
			ts := time.Unix(0, timestamp)
			pdb.Database.Insert(seriesName, ts, value)
			loadedPoints++
			fmt.Printf("Loaded point: series=%s, ts=%d, value=%f\n", seriesName, timestamp, value)
		}
		offset += int64(pos)
	}
	fmt.Printf("Loaded %d points from WAL\n", loadedPoints)

	return nil
}

// Reload reloads data from WAL, clearing existing in-memory data
func (pdb *PersistentDatabase) Reload() error {
	pdb.mu.Lock()
	// Clear existing series
	pdb.series = make(map[string]*Series)
	pdb.mu.Unlock()

	return pdb.loadFromDisk()
}

// Close closes the database and WAL file
func (pdb *PersistentDatabase) Close() error {
	// signal writer to finish and then close file
	close(pdb.done)
	// give async writer a moment to flush based on configured interval
	time.Sleep(WalFlushInterval * 2) // Increase to 100ms
	if pdb.walFile != nil {
		return pdb.walFile.Close()
	}
	return nil
}

// CompactWAL compacts the WAL by rewriting data (for now, just truncate if needed)
func (pdb *PersistentDatabase) CompactWAL() error {
	// For simplicity, just close and reopen WAL (effectively truncating)
	pdb.walFile.Close()
	walPath := filepath.Join(pdb.dataDir, "wal.log")
	return os.Remove(walPath) // Remove old WAL, new one will be created on next insert
}

// asyncWriter runs in a goroutine to write to WAL asynchronously
func (pdb *PersistentDatabase) asyncWriter() {
	// Batch buffers for fewer writes and Syncs
	batch := make([][]byte, 0, WalFlushBatchSize)
	flushTimer := time.NewTimer(WalFlushInterval)
	defer flushTimer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		// Write all items in batch in a single syscall where possible
		for _, data := range batch {
			if _, err := pdb.walFile.Write(data); err != nil {
				fmt.Println("WAL write error:", err)
			}
			// return buffer to pool
			bufferPool.Put(data[:0])
		}
		// sync once per batch
		if err := pdb.walFile.Sync(); err != nil {
			fmt.Println("WAL sync error:", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case data := <-pdb.writeChan:
			batch = append(batch, data)
			if len(batch) >= WalFlushBatchSize {
				flush()
				if !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}
				flushTimer.Reset(WalFlushInterval)
			}
		case <-flushTimer.C:
			flush()
			flushTimer.Reset(WalFlushInterval)
		case <-pdb.done:
			// drain remaining
			drain := true
			for drain {
				select {
				case data := <-pdb.writeChan:
					batch = append(batch, data)
					if len(batch) >= WalFlushBatchSize {
						flush()
					}
				default:
					drain = false
				}
			}
			flush()
			return
		}
	}
}
