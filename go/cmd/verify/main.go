// Command verify checks a finished run. It compares the jobs the load generator submitted with the effects the
// workers applied and exits non-zero if any job was lost, ran early, or ran without being submitted.
//
// Run it after the queue has drained. Jobs of type "poison" are expected to end dead-lettered; with -url, verify
// asks the server for each one's status.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/your-handle/pulse-scheduler/go/internal/verify"
	"github.com/your-handle/pulse-scheduler/go/pulse"
)

func main() {
	var (
		submittedPath = flag.String("submitted", "submitted.jsonl", "load generator output")
		completedPath = flag.String("completed", "completed.jsonl", "worker output (all workers append to it)")
		url           = flag.String("url", "", "Pulse server URL; if set, poison jobs are checked against the dead-letter state")
	)
	flag.Parse()

	submitted, err := verify.ReadJSONL[verify.Submitted](*submittedPath)
	if err != nil {
		log.Fatal(err)
	}
	completed, err := verify.ReadJSONL[verify.Completed](*completedPath)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}

	dead := map[string]bool{}
	if *url != "" {
		client := pulse.New(*url)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, s := range submitted {
			if s.Type != "poison" {
				continue
			}
			job, err := client.GetJob(ctx, s.ID)
			if err != nil {
				log.Fatalf("looking up %s: %v", s.ID, err)
			}
			if job != nil && job.Status == "DEAD" {
				dead[s.ID] = true
			}
		}
	}

	r := verify.Check(submitted, completed, dead)

	fmt.Printf("submitted:        %d\n", r.Submitted)
	fmt.Printf("executed:         %d\n", r.Completed)
	fmt.Printf("dead-lettered:    %d\n", r.DeadLettered)
	fmt.Printf("duplicate runs:   %d jobs, %d extra executions (allowed: at-least-once)\n", len(r.Duplicates), r.ExtraExecutions())
	fmt.Printf("missing:          %d\n", len(r.Missing))
	fmt.Printf("ran early:        %d\n", len(r.Early))
	fmt.Printf("never submitted:  %d\n", len(r.Unexpected))
	show("missing", r.Missing)
	show("early", r.Early)
	show("unexpected", r.Unexpected)

	if !r.OK() {
		fmt.Println("RESULT: FAIL")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS (every job accounted for, none early)")
}

func show(label string, ids []string) {
	const limit = 10
	for i, id := range ids {
		if i == limit {
			fmt.Printf("  ... and %d more %s\n", len(ids)-limit, label)
			return
		}
		fmt.Printf("  %s: %s\n", label, id)
	}
}
