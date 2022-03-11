package openwhisk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type LogLine struct {
	Message      string `json:"message,omitempty"`
	Time         string `json:"dt,omitempty"`
	Stream       string `json:"stream,omitempty"`
	ActionName   string `json:"actionName,omitempty"`
	ActivationId string `json:"activationId,omitempty"`
}

type RemoteLogger interface {
	// Send sends a logline to the remote service. Implementations can choose to batch
	// lines.
	Send(LogLine) error

	// Flush force sends all potentially batched lines. This can be used to hurry up
	// sending at the end of an activation.
	Flush() error
}

// httpLogger sends a logline per HTTP request. No batching is done.
// TODO: Is this enough for a start or do we need to invest in batching right away?
type httpLogger struct {
	http *http.Client
	url  string
	auth string
}

func (l *httpLogger) Send(line LogLine) error {
	// TODO: Should we parse the Message attribute and flatten the JSON to allow for
	// JSON formatted user logs? We could check if the first byte is a '}' and only
	// append our metadata fields if so, assuming that users want their JSON format
	// be forwarded untouched.
	by, err := json.Marshal(line)
	if err != nil {
		return fmt.Errorf("failed to marshal logline: %w", err)
	}

	fmt.Println("sending ", string(by))
	req, err := http.NewRequest(http.MethodPost, l.url, bytes.NewBuffer(by))
	if err != nil {
		return fmt.Errorf("failed to construct HTTP request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", l.auth)

	res, err := l.http.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode >= 300 {
		return fmt.Errorf("failed to ingest log line, code: %d", res.StatusCode)
	}
	return nil
}

func (l *httpLogger) Flush() error {
	// Nothing to flush. We're sending per line anyway.
	return nil
}
