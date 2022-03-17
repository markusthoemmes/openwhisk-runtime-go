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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var m = map[string]string{}

func ExampleNewExecutor_failed() {
	log, _ := ioutil.TempFile("", "log")
	proc := NewExecutor(log, log, "true", m)
	err := proc.Start(false)
	fmt.Println(err)
	proc.Stop()
	proc = NewExecutor(log, log, "/bin/pwd", m)
	err = proc.Start(false)
	fmt.Println(err)
	proc.Stop()
	proc = NewExecutor(log, log, "donotexist", m)
	err = proc.Start(false)
	fmt.Println(err)
	proc.Stop()
	proc = NewExecutor(log, log, "/etc/passwd", m)
	err = proc.Start(false)
	fmt.Println(err)
	proc.Stop()
	// Output:
	// command exited
	// command exited
	// command exited
	// command exited
}

func ExampleNewExecutor_bc() {
	log, _ := ioutil.TempFile("", "log")
	proc := NewExecutor(log, log, "_test/bc.sh", m)
	err := proc.Start(false)
	fmt.Println(err)
	res, _ := proc.Interact([]byte("2+2"))
	fmt.Printf("%s", res)
	proc.Stop()
	dump(log)
	// Output:
	// <nil>
	// 4
}

func ExampleNewExecutor_hello() {
	log, _ := ioutil.TempFile("", "log")
	proc := NewExecutor(log, log, "_test/hello.sh", m)
	err := proc.Start(false)
	fmt.Println(err)
	res, _ := proc.Interact([]byte(`{"value":{"name":"Mike"}}`))
	fmt.Printf("%s", res)
	proc.Stop()
	dump(log)
	// Output:
	// <nil>
	// {"hello": "Mike"}
	// msg=hello Mike
}

func ExampleNewExecutor_env() {
	log, _ := ioutil.TempFile("", "log")
	proc := NewExecutor(log, log, "_test/env.sh", map[string]string{"TEST_HELLO": "WORLD", "TEST_HI": "ALL"})
	err := proc.Start(false)
	fmt.Println(err)
	res, _ := proc.Interact([]byte(`{"value":{"name":"Mike"}}`))
	fmt.Printf("%s", res)
	proc.Stop()
	dump(log)
	// Output:
	// <nil>
	// { "env": "TEST_HELLO=WORLD TEST_HI=ALL"}
}

func ExampleNewExecutor_ack() {
	log, _ := ioutil.TempFile("", "log")
	proc := NewExecutor(log, log, "_test/hi", m)
	err := proc.Start(true)
	fmt.Println(err)
	proc.Stop()
	dump(log)
	// Output:
	// Command exited abruptly during initialization.
	// hi
}

func ExampleNewExecutor_badack() {
	log, _ := ioutil.TempFile("", "log")
	proc := NewExecutor(log, log, "_test/badack.sh", m)
	err := proc.Start(true)
	fmt.Println(err)
	proc.Stop()
	dump(log)
	// Output:
	// invalid character 'b' looking for beginning of value
}

func ExampleNewExecutor_badack2() {
	log, _ := ioutil.TempFile("", "log")
	proc := NewExecutor(log, log, "_test/badack2.sh", m)
	err := proc.Start(true)
	fmt.Println(err)
	proc.Stop()
	dump(log)
	// Output:
	// The action did not initialize properly.
}

func ExampleNewExecutor_helloack() {
	log, _ := ioutil.TempFile("", "log")
	proc := NewExecutor(log, log, "_test/helloack/exec", m)
	err := proc.Start(true)
	fmt.Println(err)
	res, _ := proc.Interact([]byte(`{"value":{"name":"Mike"}}`))
	fmt.Printf("%s", res)
	proc.Stop()
	dump(log)
	// Output:
	// <nil>
	// {"hello": "Mike"}
	// msg=hello Mike
}

func TestExecutorRemoteLogging(t *testing.T) {
	lines := make(chan LogLine, 2) // We expect 2 lines being logged.
	stdout, stderr := testStdoutStderr(t)
	proc := NewExecutor(stdout, stderr, "_test/remotelogging.sh", map[string]string{logDestinationServiceEnv: "logtail", logtailTokenEnv: "foo"})
	proc.remoteLogger = testLogger(func(l LogLine) error {
		l.Time = time.Time{} // Nullify to support comparison below.
		lines <- l
		return nil
	})
	assert.NoError(t, proc.Start(true), "failed to launch process")

	_, err := proc.Interact([]byte(`{"value":{"name":"Markus"}, "activation_id": "testid", "action_name": "testaction"}`))
	assert.NoError(t, err, "failed to interact with process")

	// The streams are technically not guaranteed to arrive in order, so we have to
	// collect them asynchronous and compare ignoring the order.
	var got []LogLine
	for i := 0; i < 2; i++ {
		got = append(got, <-lines)
	}
	assert.ElementsMatch(t, []LogLine{{
		Stream:       "stdout",
		Message:      "hello from stdout",
		ActionName:   "testaction",
		ActivationId: "testid",
	}, {
		Stream:       "stderr",
		Message:      "hello from stderr",
		ActionName:   "testaction",
		ActivationId: "testid",
	}}, got)

	proc.Stop()

	// On the actual stdout and stderr, there should only be the log notice and the
	// sentinels.
	assert.Equal(t, []string{remoteLogNotice, logSentinel}, fileToLines(stdout), "local stdout not as expected")
	assert.Equal(t, []string{logSentinel}, fileToLines(stderr), "local stderr not as expected")
}

func TestExecutorRemoteLoggingError(t *testing.T) {
	stdout, stderr := testStdoutStderr(t)
	proc := NewExecutor(stdout, stderr, "_test/remotelogging.sh", map[string]string{logDestinationServiceEnv: "logtail", logtailTokenEnv: "foo"})
	proc.remoteLogger = testLogger(func(l LogLine) error {
		return errors.New("an error")
	})
	assert.NoError(t, proc.Start(true), "failed to launch process")

	_, err := proc.Interact([]byte(`{"value":{"name":"Markus"}}`))
	assert.NoError(t, err, "failed to interact with process")

	proc.Stop()

	assert.Equal(t, []string{remoteLogNotice, logSentinel}, fileToLines(stdout), "local stdout not as expected")
	assert.Equal(t, []string{"Failed to process logs: failed to send log to remote location: an error", logSentinel}, fileToLines(stderr), "local stderr not as expected")
}

func TestExecutorRemoteLoggingSetupErrors(t *testing.T) {
	tests := []struct {
		service       string
		wantErrorLine string
	}{{
		service:       "logtail",
		wantErrorLine: `Failed to setup remote logging: "LOGTAIL_SOURCE_TOKEN" has to be an environment variable of the action`,
	}, {
		service:       "papertrail",
		wantErrorLine: `Failed to setup remote logging: "PAPERTRAIL_TOKEN" has to be an environment variable of the action`,
	}, {
		service:       "datadog",
		wantErrorLine: `Failed to setup remote logging: "DD_SITE" and "DD_API_KEY" have to be an environment variable of the action`,
	}}

	for _, test := range tests {
		t.Run(test.service, func(t *testing.T) {
			stdout, stderr := testStdoutStderr(t)
			proc := NewExecutor(stdout, stderr, "_test/remotelogging.sh", map[string]string{logDestinationServiceEnv: test.service})
			assert.Nil(t, proc)
			assert.Empty(t, fileToLines(stdout), "stdout wasn't empty")
			assert.Equal(t, []string{test.wantErrorLine}, fileToLines(stderr), "stderr wasn't as expected")
		})
	}
}

type testLogger func(LogLine) error

func (l testLogger) Send(line LogLine) error {
	return l(line)
}

func (l testLogger) Flush() error {
	return nil
}

func testStdoutStderr(t *testing.T) (*os.File, *os.File) {
	tmp := t.TempDir()
	stdout, err := ioutil.TempFile(tmp, "stdout")
	assert.NoError(t, err, "failed to create stdout file")
	stderr, err := ioutil.TempFile(tmp, "stderr")
	assert.NoError(t, err, "failed to create stderr file")

	return stdout, stderr
}

func fileToLines(file *os.File) []string {
	buf, _ := ioutil.ReadFile(file.Name())
	lines := strings.Split(string(buf), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		return lines[:len(lines)-1]
	}
	return lines
}
