// Package verify checks a finished run: every submitted job must be accounted for, and nothing may run early.
package verify

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// Submitted is one line of the load generator's output.
type Submitted struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	RunAt int64  `json:"runAt"` // epoch millis the job became eligible
}

// Completed is one line of a worker's output, written when the job's effect was applied (before the ack).
type Completed struct {
	ID       string `json:"id"`
	WorkerID string `json:"workerId"`
	At       int64  `json:"at"` // epoch millis
	Attempt  int    `json:"attempt"`
}

// Report is the outcome of Check.
type Report struct {
	Submitted    int
	Completed    int // distinct jobs executed at least once
	DeadLettered int
	// Missing are jobs that were neither executed nor dead-lettered: lost, or the run was checked too early.
	Missing []string
	// Duplicates maps a job ID to how many times its effect was applied. At-least-once allows these.
	Duplicates map[string]int
	// Early are jobs executed before their runAt.
	Early []string
	// Unexpected are executions of IDs that were never submitted.
	Unexpected []string
}

// ExtraExecutions is the total number of executions beyond the first for every duplicated job.
func (r Report) ExtraExecutions() int {
	total := 0
	for _, n := range r.Duplicates {
		total += n - 1
	}
	return total
}

// OK reports whether the run upholds the guarantees: nothing lost, nothing early, nothing unexpected.
// Duplicates are reported but do not fail the run, because delivery is at-least-once.
func (r Report) OK() bool {
	return len(r.Missing) == 0 && len(r.Early) == 0 && len(r.Unexpected) == 0
}

// Check compares submissions against executions. dead holds IDs confirmed in the dead-letter state.
func Check(submitted []Submitted, completed []Completed, dead map[string]bool) Report {
	runAt := make(map[string]int64, len(submitted))
	for _, s := range submitted {
		runAt[s.ID] = s.RunAt
	}

	runs := make(map[string]int, len(completed))
	early := map[string]bool{}
	var unexpected []string
	unexpectedSeen := map[string]bool{}
	for _, c := range completed {
		at, known := runAt[c.ID]
		if !known {
			if !unexpectedSeen[c.ID] {
				unexpectedSeen[c.ID] = true
				unexpected = append(unexpected, c.ID)
			}
			continue
		}
		runs[c.ID]++
		if c.At < at {
			early[c.ID] = true
		}
	}

	r := Report{Submitted: len(runAt), Duplicates: map[string]int{}}
	for id := range runAt {
		n := runs[id]
		switch {
		case n > 0:
			r.Completed++
			if n > 1 {
				r.Duplicates[id] = n
			}
		case dead[id]:
			r.DeadLettered++
		default:
			r.Missing = append(r.Missing, id)
		}
	}
	for id := range early {
		r.Early = append(r.Early, id)
	}
	r.Unexpected = unexpected
	sort.Strings(r.Missing)
	sort.Strings(r.Early)
	sort.Strings(r.Unexpected)
	return r
}

// ReadJSONL decodes a file with one JSON value per line. Blank lines are skipped. A truncated final line, which
// a crashed process can leave behind, is ignored.
func ReadJSONL[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []T
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	line := 0
	for sc.Scan() {
		line++
		raw := sc.Bytes()
		if len(raw) == 0 {
			continue
		}
		var v T
		if err := json.Unmarshal(raw, &v); err != nil {
			if isLastLine(sc) {
				break
			}
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		out = append(out, v)
	}
	return out, sc.Err()
}

// isLastLine reports whether the scanner has no more input. Called after a bad line to tolerate a torn tail.
func isLastLine(sc *bufio.Scanner) bool {
	return !sc.Scan()
}
