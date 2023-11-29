// Copyright 2023 Google LLC
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
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/hashicorp/go-retryablehttp"
)

// Emulator starts a Cloud Datastore emulator; see
// https://cloud.google.com/datastore/docs/tools/datastore-emulator.  If the
// given context has a deadline, it is applied to the entire running time of
// the emulator process.  When the test ends, the returned client is
// automatically closed and the emulator process is killed.  Unless overridden
// by passing [Option] arguments, this function will start the emulator in
// Firestore mode and wait up to 20 seconds for it to start up.
func Emulator(ctx context.Context, t testing.TB, opts ...Option) *datastore.Client {
	t.Helper()

	o := options{
		mode:         FirestoreMode,
		startTimeout: 20 * time.Second,
	}
	for _, opt := range opts {
		opt.apply(&o)
	}

	startCtx, cancel := context.WithTimeout(ctx, o.startTimeout)
	defer cancel()

	args := []string{
		"beta", "emulators", "datastore", "start",
		"--data-dir=" + t.TempDir(),
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

	pr, pw := io.Pipe()
	envCh := make(chan envVar, 1)
	go func() {
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
		if err := pr.Close(); err != nil {
			t.Errorf("dstest: error closing output reader: %s", err)
		}
		close(envCh)
	}()
	cmd.Stdout = pw
	cmd.Stderr = pw

	t.Log("dstest: starting Cloud Datastore emulator")
	if err := cmd.Start(); err != nil {
		t.Fatalf("dstest: couldn’t start Cloud Datastore emulator: %s", err)
	}
	go func() {
		if err := cmd.Wait(); err != nil {
			t.Errorf("dstest: Cloud Datastore emulator failed: %s", err)
		}
		t.Log("dstest: Cloud Datastore emulator terminated")
	}()
	t.Cleanup(func() {
		t.Log("dstest: killing Cloud Datastore emulator")
		if err := cmd.Process.Kill(); err != nil {
			t.Errorf("dstest: killing Cloud Datastore emulator failed: %s", err)
		}
		if err := pw.Close(); err != nil {
			t.Errorf("dstest: error closing output writer: %s", err)
		}
		t.Log("dstest: Cloud Datastore emulator killed")
	})

	t.Log("dstest: Cloud Datastore emulator started; waiting for startup")
	var env envVar
	select {
	case env = <-envCh:
	case <-startCtx.Done():
		t.Fatalf("dstest: Cloud Datastore emulator didn’t start up: %s", startCtx.Err())
	}

	t.Logf("dstest: Cloud Datastore emulator running at %s; waiting for health check", env.value)
	// This could be a HEAD request, but the datastore emulator doesn’t
	// accept those.
	req, err := retryablehttp.NewRequestWithContext(startCtx, http.MethodGet, fmt.Sprintf("http://%s/", env.value), nil)
	if err != nil {
		t.Fatalf("dstest: health check failed: %s", err)
	}
	resp, err := retryablehttp.NewClient().Do(req)
	if err != nil {
		t.Fatalf("dstest: health check failed: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dstest: health checked failed with HTTP status %s", resp.Status)
	}

	t.Logf("dstest: Cloud Datastore emulator running at %s is healthy", env.value)
	t.Setenv(env.name, env.value)

	client, err := datastore.NewClient(ctx, datastore.DetectProjectID)
	if err != nil {
		t.Fatalf("dstest: error creating Cloud Datastore client: %s", err)
	}
	t.Cleanup(func() {
		t.Log("dstest: closing datastore client")
		if err := client.Close(); err != nil {
			t.Errorf("dstest: couldn’t close datastore client: %s", err)
		}
	})
	return client
}

// Option is an option for [Emulator].  The current implementations are [Mode]
// and [StartTimeout].
type Option interface {
	apply(*options)
}

// Mode is an [Option] that determines the mode of the datastore emulator;
// either Firestore or legacy mode.
type Mode int

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

func (t StartTimeout) apply(o *options) {
	o.startTimeout = time.Duration(t)
}

type options struct {
	mode         Mode
	startTimeout time.Duration
}

type envVar struct{ name, value string }

var envRegexp = regexp.MustCompile(`^\[datastore\] +export (DATASTORE_EMULATOR_HOST)=(.+)$`)
