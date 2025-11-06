// Copyright 2023, 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package dstest contains functionality to help with testing code that relies
// on the Google Cloud Datastore.
package dstest

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/facebookgo/freeport"
	"github.com/frioux/shellquote"
	"github.com/hashicorp/go-retryablehttp"
)

// Emulator starts a Cloud Datastore emulator; see
// https://cloud.google.com/datastore/docs/tools/datastore-emulator.  When the
// test ends, the returned client is automatically closed and the emulator
// process is killed.  If the given context has a deadline, it is applied to
// the entire running time of the emulator process; this means that you
// shouldn’t pass t.Context() because it’ll get cancelled before Emulator gets
// a chance to clean up the datastore process.  Unless overridden by passing
// [Option] arguments, this function will start the emulator in Firestore mode
// and wait up to 20 seconds for it to start up and stop, respectively.
func Emulator(ctx context.Context, t testing.TB, opts ...Option) *datastore.Client {
	t.Helper()

	o := options{
		mode:         FirestoreMode,
		startTimeout: 20 * time.Second,
		stopTimeout:  20 * time.Second,
	}
	for _, opt := range opts {
		opt.apply(&o)
	}

	startCtx, cancel := context.WithTimeout(ctx, o.startTimeout)
	defer cancel()

	port, err := freeport.Get()
	if err != nil {
		t.Fatal(err)
	}
	args := []string{
		"beta", "emulators", "datastore", "start",
		"--data-dir=" + t.TempDir(),
		"--host-port=localhost:" + strconv.Itoa(port),
		"--no-store-on-disk",
	}
	switch m := o.mode; m {
	case FirestoreMode:
		args = append(args, "--use-firestore-in-datastore-mode")
	case LegacyMode:
	default:
		t.Fatalf("invalid mode %v", m)
	}
	cmd := exec.CommandContext(ctx, "gcloud", args...)

	envCh := make(chan envVar, 1)
	pr, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout

	t.Log("dstest: starting Cloud Datastore emulator")
	cmdLine, err := shellquote.Quote(cmd.Args)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("dstest:", cmdLine)
	if err := cmd.Start(); err != nil {
		t.Fatalf("dstest: couldn’t start Cloud Datastore emulator: %s", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s := bufio.NewScanner(pr)
		for s.Scan() {
			line := s.Text()
			t.Log("dstest:", line)
			if m := envRegexp.FindStringSubmatch(line); m != nil {
				envCh <- envVar{m[1], m[2]}
			}
		}
		if err := s.Err(); err != nil {
			t.Errorf("dstest: couldn’t read output of Cloud Datastore emulator: %s", err)
		}
		close(envCh)
		io.Copy(io.Discard, pr)
		if err := cmd.Wait(); err != nil {
			t.Errorf("dstest: Cloud Datastore emulator failed: %s", err)
		}
		t.Log("dstest: Cloud Datastore emulator terminated")
	}()

	t.Log("dstest: Cloud Datastore emulator started; waiting for startup")
	var env envVar
	select {
	case env = <-envCh:
	case <-startCtx.Done():
		t.Fatalf("dstest: Cloud Datastore emulator didn’t start up: %s", startCtx.Err())
	}

	if env.name == "" || env.value == "" {
		t.Fatal("dstest: Cloud Datastore emulator didn’t start up")
	}
	t.Logf("dstest: Cloud Datastore emulator running at %s; waiting for health check", env.value)
	httpClient := retryablehttp.NewClient()
	httpClient.Logger = logger{t}
	// This could be a HEAD request, but the datastore emulator doesn’t
	// accept those.
	req, err := retryablehttp.NewRequestWithContext(startCtx, http.MethodGet, fmt.Sprintf("http://%s/", env.value), nil)
	if err != nil {
		t.Fatalf("dstest: health check failed: %s", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("dstest: health check failed: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dstest: health check failed with HTTP status %s", resp.Status)
	}

	t.Logf("dstest: Cloud Datastore emulator running at %s is healthy", env.value)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(ctx, o.stopTimeout)
		defer cancel()
		t.Log("dstest: asking Cloud Datastore emulator to shut down")
		req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/shutdown", env.value), nil)
		if err != nil {
			t.Logf("dstest: stopping Cloud Datastore emulator failed: %s", err)
			return
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Logf("dstest: stopping Cloud Datastore emulator failed: %s", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Logf("dstest: stopping Cloud Datastore emulator failed: %s", resp.Status)
			return
		}
		t.Log("dstest: waiting for Cloud Datastore emulator to stop")
		wg.Wait()
	})
	t.Setenv(env.name, env.value)

	client, err := datastore.NewClient(ctx, datastore.DetectProjectID)
	if err != nil {
		t.Fatalf("dstest: error creating Cloud Datastore client: %s", err)
	}
	t.Cleanup(func() {
		t.Log("dstest: closing datastore client")
		if err := client.Close(); err != nil {
			t.Logf("dstest: couldn’t close datastore client: %s", err)
		}
	})
	return client
}

// Option is an option for [Emulator].  The current implementations are [Mode],
// [StartTimeout], and [StopTimeout].
type Option interface {
	apply(*options)
}

// Mode is an [Option] that determines the mode of the datastore emulator;
// either Firestore or legacy mode.
type Mode int

var _ Option = Mode(0)

func (m Mode) apply(o *options) {
	o.mode = m
}

const (
	FirestoreMode Mode = iota
	LegacyMode
)

// StartTimeout is an [Option] that determines how long to wait for the
// datastore emulator to start.
type StartTimeout time.Duration

var _ Option = StartTimeout(0)

func (t StartTimeout) apply(o *options) {
	o.startTimeout = time.Duration(t)
}

// StopTimeout is an [Option] that determines how long to wait for the
// datastore emulator to stop.
type StopTimeout time.Duration

var _ Option = StopTimeout(0)

func (t StopTimeout) apply(o *options) {
	o.stopTimeout = time.Duration(t)
}

type options struct {
	mode                      Mode
	startTimeout, stopTimeout time.Duration
}

type envVar struct{ name, value string }

var envRegexp = regexp.MustCompile(`^\[datastore\] +export (DATASTORE_EMULATOR_HOST)=(.+)$`)
