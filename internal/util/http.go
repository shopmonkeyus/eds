package util

import (
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/exp/rand"
)

const maxAttempts = 3

type HTTPRetry struct {
	attempts int
	req      *http.Request
}

func (r *HTTPRetry) shouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "connection reset") || strings.Contains(msg, "connection refused") {
			return r.attempts <= maxAttempts
		}
	}
	if resp != nil {
		switch resp.StatusCode {
		case http.StatusRequestTimeout, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusTooManyRequests:
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return true
		}
	}
	return false
}

func (r *HTTPRetry) Do() (*http.Response, error) {
	r.attempts++
	resp, err := http.DefaultClient.Do(r.req)
	if r.shouldRetry(resp, err) {
		jitter := time.Duration(time.Millisecond*100 + time.Millisecond*time.Duration(rand.Int63n(int64(500*r.attempts))))
		time.Sleep(jitter)
		return r.Do()
	}
	return resp, err
}

// NewHTTPRetry creates a new utility for retrying HTTP requests.
func NewHTTPRetry(req *http.Request) *HTTPRetry {
	return &HTTPRetry{
		req: req,
	}
}
