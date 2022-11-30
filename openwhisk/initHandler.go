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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/apache/openwhisk-runtime-go/openwhisk/logging"
)

type initBodyRequest struct {
	Code   string                 `json:"code,omitempty"`
	Binary bool                   `json:"binary,omitempty"`
	Main   string                 `json:"main,omitempty"`
	Env    map[string]interface{} `json:"env,omitempty"`
}

type initRequest struct {
	Value initBodyRequest `json:"value,omitempty"`
}

func sendOK(w http.ResponseWriter) {
	// answer OK
	w.Header().Set("Content-Type", "application/json")
	buf := []byte("{\"ok\":true}\n")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(buf)))
	w.Write(buf)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func (ap *ActionProxy) initHandler(w http.ResponseWriter, r *http.Request) {

	// you can do multiple initializations when debugging
	if ap.initialized && !Debugging {
		msg := "Cannot initialize the action more than once."
		sendError(w, http.StatusForbidden, msg)
		log.Println(msg)
		return
	}

	// read body of the request
	if ap.compiler != "" {
		Debug("compiler: " + ap.compiler)
	}

	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("%v", err))
		return
	}

	// decode request parameters
	if len(body) < 1000 {
		Debug("init: decoding %s\n", string(body))
	}

	var request initRequest
	err = json.Unmarshal(body, &request)
	if err != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Error unmarshaling request: %v", err))
		return
	}

	// request with empty code - stop any executor but return ok
	if request.Value.Code == "" {
		sendError(w, http.StatusForbidden, "Missing main/no code to execute.")
		return
	}

	if os.Getenv("OW_INIT_IN_ACTIONLOOP") == "" {
		if ap.theExecutor != nil {
			// Stop the prelaunched binary since we're going to start a fresh one.
			ap.theExecutor.Stop()
		}
		err = ap.scriptBasedInit(request)
	} else {
		err = ap.actionloopBasedInit(body, request)
	}

	if err != nil {
		sendError(w, http.StatusBadGateway, err.Error())
		return
	}
	ap.initialized = true
	sendOK(w)
}

func (ap *ActionProxy) scriptBasedInit(request initRequest) error {
	// passing the env to the action proxy
	ap.SetEnv(request.Value.Env)

	// setting main
	main := request.Value.Main
	if main == "" {
		main = "main"
	}

	// extract code eventually decoding it
	var buf []byte
	if request.Value.Binary {
		Debug("it is binary code")
		var err error
		buf, err = base64.StdEncoding.DecodeString(request.Value.Code)
		if err != nil {
			return fmt.Errorf("cannot decode the request: %w", err)
		}
	} else {
		Debug("it is source code")
		buf = []byte(request.Value.Code)
	}

	// if a compiler is defined try to compile
	_, err := ap.ExtractAndCompile(&buf, main)
	if err != nil {
		if os.Getenv("OW_LOG_INIT_ERROR") == "" {
			return fmt.Errorf("cannot initialize action: %w", err)
		}
		ap.errFile.Write([]byte(err.Error() + "\n"))
		ap.outFile.Write([]byte(OutputGuard))
		ap.errFile.Write([]byte(OutputGuard))
		return errors.New("The action failed to generate or locate a binary. See logs for details.")
	}

	// start an action
	err = ap.StartLatestAction()
	if err != nil {
		if os.Getenv("OW_LOG_INIT_ERROR") == "" {
			return fmt.Errorf("cannot start action: %w", err)
		}
		ap.errFile.Write([]byte(err.Error() + "\n"))
		ap.outFile.Write([]byte(OutputGuard))
		ap.errFile.Write([]byte(OutputGuard))
		return errors.New("Cannot start action. Check logs for details.")
	}
	return nil
}

func (ap *ActionProxy) actionloopBasedInit(rawRequest []byte, parsedRequest initRequest) error {
	// Populate the environment to the action proxy's internal environment. This does not propagate
	// the environment to the process but processes them in the same way.
	ap.SetEnv(parsedRequest.Value.Env)
	loggers, err := logging.RemoteLoggerFromEnv(ap.env)
	if err != nil {
		return fmt.Errorf("failed to setup loggers: %w", err)
	}

	// If the init payload had configuration for new loggers, override the default ones now.
	if len(loggers) > 0 {
		ap.theExecutor.isRemoteLogging = true
		ap.theExecutor.loggers = loggers
	}

	out, err := ap.theExecutor.Interact(rawRequest, true)
	if err != nil {
		if os.Getenv("OW_LOG_INIT_ERROR") == "" {
			return fmt.Errorf("cannot initialize action: %w", err)
		}
		ap.errFile.Write([]byte(err.Error() + "\n"))
		ap.outFile.Write([]byte(OutputGuard))
		ap.errFile.Write([]byte(OutputGuard))
		return errors.New("Cannot initialize action. Check logs for details.")
	}

	var ackData ActionAck
	if err := json.Unmarshal(out, &ackData); err != nil {
		return err
	}
	if !ackData.Ok {
		return errors.New("The action did not initialize properly.")
	}

	return nil
}

// ExtractAndCompile decode the buffer and if a compiler is defined, compile it also
func (ap *ActionProxy) ExtractAndCompile(buf *[]byte, main string) (string, error) {

	// extract action in src folder
	file, err := ap.ExtractAction(buf, "src")
	if err != nil {
		return "", err
	}
	if file == "" {
		return "", fmt.Errorf("empty filename")
	}

	// some path surgery
	dir := filepath.Dir(file)
	parent := filepath.Dir(dir)
	srcDir := filepath.Join(parent, "src")
	binDir := filepath.Join(parent, "bin")
	binFile := filepath.Join(binDir, "exec")

	// if the file is already compiled or there is no compiler just move it from src to bin
	if ap.compiler == "" || isCompiled(file) {
		os.Rename(srcDir, binDir)
		return binFile, nil
	}

	// ok let's try to compile
	Debug("compiling: %s main: %s", file, main)
	os.Mkdir(binDir, 0755)
	err = ap.CompileAction(main, srcDir, binDir)
	if err != nil {
		return "", err
	}

	// check only if the file exist
	if _, err := os.Stat(binFile); os.IsNotExist(err) {
		return "", fmt.Errorf("cannot compile")
	}
	return binFile, nil
}
