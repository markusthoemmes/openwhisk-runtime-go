/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package openwhisk

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// OutputGuard constant string
const OutputGuard = "XXX_THE_END_OF_A_WHISK_ACTIVATION_XXX\n"

// DefaultTimeoutStart to wait for a process to start
var DefaultTimeoutStart = 5 * time.Millisecond

type activationMetadata struct {
	ActivationId string `json:"activation_id"`
	ActionName   string `json:"action_name"`
}

// Executor is the container and the guardian  of a child process
// It starts a command, feeds input and output, read logs and control its termination
type Executor struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	output *bufio.Reader
	exited chan bool

	stdout       *bufio.Reader
	stderr       *bufio.Reader
	remoteLogger RemoteLogger

	logout *os.File
	logerr *os.File
}

// NewExecutor creates a child subprocess using the provided command line,
// writing the logs in the given file.
// You can then start it getting a communication channel
func NewExecutor(logout *os.File, logerr *os.File, command string, env map[string]string, args ...string) (proc *Executor) {
	cmd := exec.Command(command, args...)
	cmd.Stdout = logout
	cmd.Stderr = logerr
	cmd.Env = []string{}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	Debug("env: %v", cmd.Env)
	if Debugging {
		cmd.Env = append(cmd.Env, "OW_DEBUG=/tmp/action.log")
	}
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil
	}
	pipeOut, pipeIn, err := os.Pipe()
	if err != nil {
		return nil
	}
	cmd.ExtraFiles = []*os.File{pipeIn}
	output := bufio.NewReader(pipeOut)
	e := &Executor{
		cmd:    cmd,
		input:  input,
		output: output,
		exited: make(chan bool),
		logout: logout,
		logerr: logerr,
	}

	// TODO: Where to surface configuration errors (unsupported destinations etc)? Ideally at action creation time.
	if env["remote_logging"] != "" {
		if err := e.setupRemoteLogging(env); err != nil {
			return nil
		}
	}

	return e
}

func (proc *Executor) setupRemoteLogging(env map[string]string) error {
	proc.cmd.Stdout = nil
	stdout, err := proc.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}
	proc.stdout = bufio.NewReader(stdout)

	proc.cmd.Stderr = nil
	stderr, err := proc.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}
	proc.stderr = bufio.NewReader(stderr)

	if env["remote_logging"] == "logtail" {
		proc.remoteLogger = &httpLogger{
			url:  "https://in.logtail.com",
			auth: fmt.Sprintf("Bearer %s", env["LOGTAIL_TOKEN"]),
			http: http.DefaultClient,
		}
	} else if env["remote_logging"] == "papertrail" {
		proc.remoteLogger = &httpLogger{
			url:  "https://logs.collector.solarwinds.com/v1/log",
			auth: fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(":"+env["PAPERTRAIL_TOKEN"]))),
			http: http.DefaultClient,
		}
	}
	return nil
}

// Interact interacts with the underlying process
func (proc *Executor) Interact(in []byte) ([]byte, error) {
	if proc.stdout == nil {
		// Remote logging is not configured. Just roundtrip and return immediately.
		return proc.roundtrip(in)
	}

	// Fetch metadata from the incoming parameters
	var metadata activationMetadata
	if err := json.Unmarshal(in, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse activation metadata: %w", err)
	}

	// A chunky buffer in order to not block execution of the function from sending off loglines.
	logsToSend := make(chan LogLine, 128)
	consumeStream := func(streamName string, stream *bufio.Reader, to io.Writer) error {
		for {
			out, err := stream.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("failed to read from stream %q: %w", streamName, err)
			}

			line := string(out)
			if line == OutputGuard {
				// Swallow the sentinel. It's written explicitly again below.
				return nil
			}

			logsToSend <- LogLine{
				// TODO: This is the format as expected by Logtail. Maybe move the formatting inside a specialized logger for each service.
				Time:         time.Now().UTC().Format("2006-01-02 15:04:05.000000000 MST"),
				Message:      strings.TrimSpace(line),
				Stream:       streamName,
				ActivationId: metadata.ActivationId,
				ActionName:   metadata.ActionName,
			}
		}
	}

	var grpConsuming errgroup.Group
	grpConsuming.Go(func() error { return consumeStream("stdout", proc.stdout, proc.logout) })
	grpConsuming.Go(func() error { return consumeStream("stderr", proc.stderr, proc.logerr) })

	var grpSending errgroup.Group
	grpSending.Go(func() error {
		for log := range logsToSend {
			if err := proc.remoteLogger.Send(log); err != nil {
				return fmt.Errorf("failed to send log to remote location: %w", err)
			}
		}
		return nil
	})

	proc.logout.WriteString("Logs will be written to the specified remote location. Only errors in doing so will be surfaced here.\n")
	out, err := proc.roundtrip(in)

	// Wait for the streams to process completely.
	if grpErr := grpConsuming.Wait(); grpErr != nil {
		fmt.Fprintf(proc.logerr, "Failed to read logs from streams: %v\n", err)
		err = grpErr
	}

	// After the consumers are done (have seen the sentinel), we know it's safe to close the channel.
	close(logsToSend)
	if grpErr := grpSending.Wait(); grpErr != nil {
		fmt.Fprintf(proc.logerr, "Failed to write logs to remote location: %v\n", err)
		err = grpErr
	}

	if err := proc.remoteLogger.Flush(); err != nil {
		fmt.Fprintf(proc.logerr, "Failed to flush logs to remote location: %v\n", err)
	}

	// Write our own sentinels instead of forwarding from the child. This makes sure that any
	// error logs we might've written are captured correctly.
	proc.logout.WriteString(OutputGuard)
	proc.logerr.WriteString(OutputGuard)

	return out, err
}

// roundtrip writes the input to the subprocess and waits for a response.
func (proc *Executor) roundtrip(in []byte) ([]byte, error) {
	proc.input.Write(in)
	proc.input.Write([]byte("\n"))

	chout := make(chan []byte)
	go func() {
		out, err := proc.output.ReadBytes('\n')
		if err == nil {
			chout <- out
		} else {
			chout <- []byte{}
		}
	}()
	var err error
	var out []byte
	select {
	case out = <-chout:
		if len(out) == 0 {
			err = errors.New("no answer from the action")
		}
	case <-proc.exited:
		err = errors.New("command exited")
	}
	return out, err
}

// Exited checks if the underlying command exited
func (proc *Executor) Exited() bool {
	select {
	case <-proc.exited:
		return true
	default:
		return false
	}
}

// ActionAck is the expected data structure for the action acknowledgement
type ActionAck struct {
	Ok bool `json:"ok"`
}

// Start execution of the command
// if the flag ack is true, wait forever for an acknowledgement
// if the flag ack is false wait a bit to check if the command exited
// returns an error if the program fails
func (proc *Executor) Start(waitForAck bool) error {
	// start the underlying executable
	Debug("Start:")
	err := proc.cmd.Start()
	if err != nil {
		Debug("run: early exit")
		proc.cmd = nil // no need to kill
		return fmt.Errorf("command exited")
	}
	Debug("pid: %d", proc.cmd.Process.Pid)

	go func() {
		proc.cmd.Wait()
		proc.exited <- true
	}()

	// not waiting for an ack, so use a timeout
	if !waitForAck {
		select {
		case <-proc.exited:
			return fmt.Errorf("command exited")
		case <-time.After(DefaultTimeoutStart):
			return nil
		}
	}

	// wait for acknowledgement
	Debug("waiting for an ack")
	ack := make(chan error)
	go func() {
		out, err := proc.output.ReadBytes('\n')
		Debug("received ack %s", out)
		if err != nil {
			ack <- err
			return
		}
		// parse ack
		var ackData ActionAck
		err = json.Unmarshal(out, &ackData)
		if err != nil {
			ack <- err
			return
		}
		// check ack
		if !ackData.Ok {
			ack <- fmt.Errorf("The action did not initialize properly.")
			return
		}
		ack <- nil
	}()
	// wait for ack or unexpected termination
	select {
	// ack received
	case err = <-ack:
		return err
	// process exited
	case <-proc.exited:
		return fmt.Errorf("Command exited abruptly during initialization.")
	}
}

// Stop will kill the process
// and close the channels
func (proc *Executor) Stop() {
	Debug("stopping")
	if proc.cmd != nil {
		proc.cmd.Process.Kill()
		proc.cmd = nil
	}
}
