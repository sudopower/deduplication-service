package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
)

// Deduplicator holds the state for tracking seen message keys.
type Deduplicator struct {
	// seen stores the last time a key was observed.
	// The key is of type interface{} to handle various JSON types (numbers, strings).
	seen map[interface{}]struct{}
}

// NewDeduplicator creates and initializes a new Deduplicator instance.
func NewDeduplicator() *Deduplicator {
	d := &Deduplicator{
		seen: make(map[interface{}]struct{}),
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
		if !d.isDuplicate(string(lineBytes)) {
			// If not a duplicate, write the original message to the output.
			fmt.Fprintln(writer, string(lineBytes))
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from STDIN: %v", err)
	}
}

// isDuplicate checks if a key has been seen before.
// It returns true if the message is a duplicate, and false otherwise.
// If the message is not a duplicate, it records the key.
func (d *Deduplicator) isDuplicate(key interface{}) bool {
	if _, found := d.seen[key]; found {
		return true
	}

	// Otherwise, it's not a duplicate. Store it.
	d.seen[key] = struct{}{}
	return false
}

func main() {

	// Create and run the service
	log.Println("Starting deduplication service")

	deduplicator := NewDeduplicator()
	deduplicator.ProcessMessages(os.Stdout, os.Stdin)

	log.Println("Deduplication service finished.")
}
