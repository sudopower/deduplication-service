package main

import (
	"bufio"
	"crypto/sha256"
	"flag"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// Deduplicator holds the state for tracking seen message keys.
// It is safe for concurrent use.
type Deduplicator struct {

	// The duration to keep a key before it can be seen again.
	// A zero value means permanent deduplication.
	period time.Duration

	// mu protects the seen map from concurrent access.
	mu sync.RWMutex

	// seen stores the last time a key was observed.
	// The key is of type interface{} to handle various types (numbers, strings).
	seen map[interface{}]time.Time

	hasher hash.Hash
}

// NewDeduplicator creates and initializes a new Deduplicator instance.
// It also starts a background cleanup goroutine if a deduplication period is specified.
func NewDeduplicator(period time.Duration) *Deduplicator {
	d := &Deduplicator{
		period: period,
		seen:   make(map[interface{}]time.Time),
		hasher: sha256.New(),
	}

	// If a period is set, we need to periodically clean up old entries
	// from the 'seen' map to prevent memory from growing indefinitely.
	if period > 0 {
		go d.startCleanup()
	}

	return d
}

// ProcessMessages reads messages from the provided reader, deduplicates them,
// and writes the unique messages to the writer.
func (d *Deduplicator) ProcessMessages(writer io.Writer, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		lineBytes := scanner.Bytes()

		// An empty line is not a valid JSON, so we skip it.
		if len(lineBytes) == 0 {
			continue
		}

		// Check if the key is a duplicate.
		d.hasher.Reset()
		d.hasher.Write(lineBytes)
		key := d.hasher.Sum(nil)
		if !d.isDuplicate(string(key)) {
			// If not a duplicate, write the original message to the output.
			fmt.Fprintln(writer, string(lineBytes))
		}

	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from STDIN: %v", err)
	}
}

// isDuplicate checks if a key has been seen before within the deduplication period.
// It returns true if the message is a duplicate, and false otherwise.
// If the message is not a duplicate, it records the key and the current time.
func (d *Deduplicator) isDuplicate(key interface{}) bool {
	// Permanent deduplication (period is 0)
	if d.period == 0 {
		d.mu.Lock()
		defer d.mu.Unlock()

		if _, found := d.seen[key]; found {
			return true // Found, so it's a duplicate.
		}
		d.seen[key] = time.Time{} // Store it permanently. The time value doesn't matter.
		return false
	}

	// Timed deduplication
	d.mu.Lock()
	defer d.mu.Unlock()

	lastSeen, found := d.seen[key]
	now := time.Now()

	// If found and the time since last seen is less than the period, it's a duplicate.
	if found && now.Sub(lastSeen) < d.period {
		return true
	}

	// Otherwise, it's not a duplicate. Record the time we saw it.
	d.seen[key] = now
	return false
}

// startCleanup runs a periodic task to remove expired keys from the 'seen' map.
// This prevents the map from growing indefinitely in timed deduplication mode.
func (d *Deduplicator) startCleanup() {
	// The cleanup interval can be the same as the period.
	// For very long periods (e.g., 1 week), you might choose a shorter interval
	// like once an hour. For this implementation, the period itself is a good default.
	ticker := time.NewTicker(d.period)
	defer ticker.Stop()

	for range ticker.C {
		d.cleanupExpired()
	}
}

// cleanupExpired iterates over the map and removes keys that are older than the period.
func (d *Deduplicator) cleanupExpired() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	for key, lastSeen := range d.seen {
		if now.Sub(lastSeen) > d.period {
			delete(d.seen, key)
		}
	}
}

func main() {
	// Define and parse command-line flags
	dedupPeriod := flag.Duration("period", 0, "Optional: Deduplication period (e.g., '10s', '5m', '1h'). If not set, deduplication is permanent.")

	flag.Parse()

	// Validate inputs
	if *dedupPeriod < 0 {
		log.Println("Error: The -period cannot be negative.")
		os.Exit(1)
	}

	// Create and run the service
	log.Printf("Starting deduplication service with period '%v'", *dedupPeriod)

	deduplicator := NewDeduplicator(*dedupPeriod)
	deduplicator.ProcessMessages(os.Stdout, os.Stdin)

	log.Println("Deduplication service finished.")
}
