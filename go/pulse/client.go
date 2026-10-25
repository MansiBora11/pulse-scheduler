// Package pulse is a small client for the Pulse job scheduler's HTTP API.
package pulse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Job is a job as returned by the server. LeaseToken is only set on jobs returned by Poll.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	RunAt       int64           `json:"runAt"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"maxAttempts"`
	LeaseToken  string          `json:"leaseToken,omitempty"`
	LeaseUntil  int64           `json:"leaseUntil"`
	LastError   string          `json:"lastError,omitempty"`
}

// SubmitRequest describes a new job. Set ID to make retries idempotent.
type SubmitRequest struct {
	ID          string `json:"id,omitempty"`
	Type        string `json:"type"`
	Payload     any    `json:"payload,omitempty"`
	RunAt       int64  `json:"runAt,omitempty"` // epoch millis; zero means now
	MaxAttempts int    `json:"maxAttempts,omitempty"`
	Priority    int    `json:"priority,omitempty"`
}

// Client talks to one Pulse server. It is safe for concurrent use.
type Client struct {
	baseURL string
	http    *http.Client
}

// New returns a client for the server at baseURL, for example "http://localhost:8080".
func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http: &http.Client{
			Timeout: 35 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        512,
				MaxIdleConnsPerHost: 512,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// Submit creates a job. created is false when the ID already existed and the stored job was returned.
func (c *Client) Submit(ctx context.Context, req SubmitRequest) (job Job, created bool, err error) {
	status, err := c.do(ctx, http.MethodPost, "/v1/jobs", req, &job)
	if err != nil {
		return Job{}, false, err
	}
	return job, status == http.StatusCreated, nil
}

// Poll claims one due job, or returns (nil, nil) when none is ready.
func (c *Client) Poll(ctx context.Context, workerID string, types []string, leaseMillis int64) (*Job, error) {
	in := map[string]any{"workerId": workerID, "types": types, "leaseMs": leaseMillis}
	var job Job
	status, err := c.do(ctx, http.MethodPost, "/v1/jobs/poll", in, &job)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		return nil, nil
	}
	return &job, nil
}

// Heartbeat extends a lease. ok is false when the lease was lost and the caller must stop working on the job.
func (c *Client) Heartbeat(ctx context.Context, id, leaseToken string, leaseMillis int64) (ok bool, err error) {
	return c.leaseCall(ctx, id, "heartbeat", map[string]any{"leaseToken": leaseToken, "leaseMs": leaseMillis})
}

// Ack marks the job done. ok is false when the lease was lost, in which case the job may run again.
func (c *Client) Ack(ctx context.Context, id, leaseToken string) (ok bool, err error) {
	return c.leaseCall(ctx, id, "ack", map[string]any{"leaseToken": leaseToken})
}

// Fail reports a failed attempt. The server retries with backoff or dead-letters the job.
func (c *Client) Fail(ctx context.Context, id, leaseToken, reason string) (ok bool, err error) {
	return c.leaseCall(ctx, id, "fail", map[string]any{"leaseToken": leaseToken, "error": reason})
}

// GetJob returns the stored job, or (nil, nil) if it does not exist.
func (c *Client) GetJob(ctx context.Context, id string) (*Job, error) {
	var job Job
	status, err := c.do(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &job)
	if status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (c *Client) leaseCall(ctx context.Context, id, action string, in any) (bool, error) {
	status, err := c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/"+action, in, nil)
	if err != nil {
		return false, err
	}
	return status != http.StatusConflict, nil
}

// do sends a request and decodes a 2xx JSON body into out. A 409 is returned as (409, nil) because it is an
// expected answer for lease operations; every other non-2xx status becomes an error.
func (c *Client) do(ctx context.Context, method, path string, in, out any) (int, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return 0, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	switch {
	case resp.StatusCode/100 == 2:
		if out != nil && len(data) > 0 {
			if err := json.Unmarshal(data, out); err != nil {
				return resp.StatusCode, fmt.Errorf("%s %s: bad response: %w", method, path, err)
			}
		}
		return resp.StatusCode, nil
	case resp.StatusCode == http.StatusConflict:
		return resp.StatusCode, nil
	default:
		return resp.StatusCode, fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, bytes.TrimSpace(data))
	}
}
