// Command worker is a Go worker for Pulse with fake handlers that exercise the scheduler's failure paths.
//
// Handler types:
//
//	fast     returns immediately
//	slow     sleeps 50-500 ms
//	flaky    fails about 20% of the time (exercises retry and backoff)
//	poison   always fails (ends up in the dead-letter queue)
//	crasher  applies its effect, then with probability -crash-pct kills the process before the ack, which
//	         leaves a lease to expire and the job to run again elsewhere
//
// Every applied effect is appended to -out so cmd/verify can compare it with what was submitted.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/your-handle/pulse-scheduler/go/internal/verify"
	"github.com/your-handle/pulse-scheduler/go/pulse"
)

type handler func(ctx context.Context, job *pulse.Job) error

var handlers = map[string]handler{
	"fast": func(context.Context, *pulse.Job) error { return nil },
	"slow": func(ctx context.Context, _ *pulse.Job) error {
		return sleep(ctx, time.Duration(50+rand.Intn(450))*time.Millisecond)
	},
	"flaky": func(context.Context, *pulse.Job) error {
		if rand.Intn(100) < 20 {
			return errors.New("simulated transient failure")
		}
		return nil
	},
	"poison":  func(context.Context, *pulse.Job) error { return errors.New("poison job always fails") },
	"crasher": func(context.Context, *pulse.Job) error { return nil },
}

type worker struct {
	client   *pulse.Client
	id       string
	types    []string
	leaseMs  int64
	crashPct int

	outMu sync.Mutex
	out   *os.File
}

func main() {
	var (
		url         = flag.String("url", "http://localhost:8080", "Pulse server URL")
		id          = flag.String("id", fmt.Sprintf("go-%d", os.Getpid()), "worker id")
		concurrency = flag.Int("concurrency", 8, "parallel polling loops")
		lease       = flag.Duration("lease", 10*time.Second, "lease duration requested per job")
		outPath     = flag.String("out", "completed.jsonl", "file recording applied effects (appended)")
		crashPct    = flag.Int("crash-pct", 0, "percent chance a crasher job kills this process before acking")
	)
	flag.Parse()

	out, err := os.OpenFile(*outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	w := &worker{
		client:   pulse.New(*url),
		id:       *id,
		leaseMs:  lease.Milliseconds(),
		crashPct: *crashPct,
		out:      out,
	}
	for t := range handlers {
		w.types = append(w.types, t)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx)
		}()
	}
	log.Printf("%s polling %s with %d loops, lease %s", w.id, *url, *concurrency, *lease)
	wg.Wait()
}

func (w *worker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		job, err := w.client.Poll(ctx, w.id, w.types, w.leaseMs)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				log.Printf("poll: %v", err)
			}
			_ = sleep(ctx, time.Second)
		case job == nil:
			_ = sleep(ctx, time.Duration(100+rand.Intn(200))*time.Millisecond)
		default:
			w.process(ctx, job)
		}
	}
}

func (w *worker) process(ctx context.Context, job *pulse.Job) {
	h := handlers[job.Type]
	if h == nil {
		w.report(ctx, job, "no handler for type "+job.Type)
		return
	}

	// Keep the lease alive while the handler runs. If the server says the lease is gone, cancel the handler.
	jctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lost atomic.Bool
	go func() {
		t := time.NewTicker(time.Duration(w.leaseMs/3) * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-jctx.Done():
				return
			case <-t.C:
				ok, err := w.client.Heartbeat(jctx, job.ID, job.LeaseToken, w.leaseMs)
				if err == nil && !ok {
					lost.Store(true)
					cancel()
					return
				}
			}
		}
	}()

	err := h(jctx, job)
	cancel()
	if lost.Load() {
		log.Printf("lease lost for %s; abandoning", job.ID)
		return
	}
	if err != nil {
		w.report(ctx, job, err.Error())
		return
	}

	w.record(job) // the "side effect"

	if job.Type == "crasher" && rand.Intn(100) < w.crashPct {
		log.Printf("simulated crash after applying %s, before ack", job.ID)
		os.Exit(1)
	}
	if ok, err := w.client.Ack(ctx, job.ID, job.LeaseToken); err != nil {
		log.Printf("ack %s: %v", job.ID, err)
	} else if !ok {
		log.Printf("ack rejected for %s: lease was lost, job may run again", job.ID)
	}
}

func (w *worker) report(ctx context.Context, job *pulse.Job, reason string) {
	if _, err := w.client.Fail(ctx, job.ID, job.LeaseToken, reason); err != nil && ctx.Err() == nil {
		log.Printf("fail %s: %v", job.ID, err)
	}
}

// record appends one line per applied effect. A single O_APPEND write per line keeps lines intact even when
// several worker processes share the file.
func (w *worker) record(job *pulse.Job) {
	line, _ := json.Marshal(verify.Completed{
		ID: job.ID, WorkerID: w.id, At: time.Now().UnixMilli(), Attempt: job.Attempts,
	})
	line = append(line, '\n')
	w.outMu.Lock()
	defer w.outMu.Unlock()
	if _, err := w.out.Write(line); err != nil {
		log.Fatalf("cannot record completion: %v", err)
	}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
