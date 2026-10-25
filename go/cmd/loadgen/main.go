// Command loadgen submits jobs to a Pulse server and records every accepted job so that cmd/verify can
// later prove nothing was lost.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/your-handle/pulse-scheduler/go/internal/verify"
	"github.com/your-handle/pulse-scheduler/go/pulse"
)

type weighted struct {
	jobType string
	weight  int
}

func main() {
	var (
		url         = flag.String("url", "http://localhost:8080", "Pulse server URL")
		total       = flag.Int("n", 1000, "number of jobs to submit")
		rate        = flag.Float64("rate", 0, "target submissions per second (0 = as fast as possible)")
		concurrency = flag.Int("concurrency", 16, "parallel submitters")
		mix         = flag.String("mix", "fast:70,slow:20,flaky:10", "job types and weights, e.g. fast:80,poison:5")
		delayMaxMs  = flag.Int("delay-max-ms", 0, "give each job a random runAt delay up to this many ms")
		maxAttempts = flag.Int("max-attempts", 5, "maxAttempts per job")
		out         = flag.String("out", "submitted.jsonl", "file recording accepted jobs")
	)
	flag.Parse()

	types, err := parseMix(*mix)
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	client := pulse.New(*url)
	ctx := context.Background()

	var (
		mu        sync.Mutex // guards w and latencies
		latencies = make([]time.Duration, 0, *total)
		failed    atomic.Int64
		jobs      = make(chan int)
	)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				req := pulse.SubmitRequest{
					ID:          newID(),
					Type:        pick(types),
					Payload:     map[string]any{"n": 1},
					MaxAttempts: *maxAttempts,
				}
				if *delayMaxMs > 0 {
					req.RunAt = time.Now().UnixMilli() + int64(mrand.Intn(*delayMaxMs+1))
				}
				job, took, err := submitWithRetry(ctx, client, req)
				if err != nil {
					failed.Add(1)
					log.Printf("giving up on %s: %v", req.ID, err)
					continue
				}
				line, _ := json.Marshal(verify.Submitted{ID: job.ID, Type: job.Type, RunAt: job.RunAt})
				mu.Lock()
				latencies = append(latencies, took)
				w.Write(line)
				w.WriteByte('\n')
				mu.Unlock()
			}
		}()
	}

	for i := 0; i < *total; i++ {
		if *rate > 0 {
			time.Sleep(time.Until(start.Add(time.Duration(float64(i) / *rate * float64(time.Second)))))
		}
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	mu.Lock()
	defer mu.Unlock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("submitted %d jobs in %s (%.0f jobs/s), %d failed\n",
		len(latencies), elapsed.Round(time.Millisecond), float64(len(latencies))/elapsed.Seconds(), failed.Load())
	fmt.Printf("submit latency  p50=%s  p95=%s  p99=%s\n",
		percentile(latencies, 0.50), percentile(latencies, 0.95), percentile(latencies, 0.99))
	fmt.Printf("accepted jobs recorded in %s\n", *out)
	if failed.Load() > 0 {
		os.Exit(1)
	}
}

// submitWithRetry reuses the same job ID on every attempt, so a retry after a lost response cannot create a
// duplicate job: the server answers with the job it already stored.
func submitWithRetry(ctx context.Context, c *pulse.Client, req pulse.SubmitRequest) (pulse.Job, time.Duration, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		t0 := time.Now()
		job, _, err := c.Submit(ctx, req)
		if err == nil {
			return job, time.Since(t0), nil
		}
		lastErr = err
		time.Sleep(time.Duration(50<<attempt) * time.Millisecond)
	}
	return pulse.Job{}, 0, lastErr
}

func parseMix(s string) ([]weighted, error) {
	var out []weighted
	for _, part := range strings.Split(s, ",") {
		name, weight, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("bad -mix entry %q, want type:weight", part)
		}
		n, err := strconv.Atoi(weight)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("bad weight in -mix entry %q", part)
		}
		out = append(out, weighted{name, n})
	}
	return out, nil
}

func pick(types []weighted) string {
	sum := 0
	for _, t := range types {
		sum += t.weight
	}
	n := mrand.Intn(sum)
	for _, t := range types {
		if n < t.weight {
			return t.jobType
		}
		n -= t.weight
	}
	return types[0].jobType
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx].Round(10 * time.Microsecond)
}

func newID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
