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

package dstest_test

import (
	"context"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/google/go-cmp/cmp"
	"github.com/phst/dstest"
)

func TestEmulator(t *testing.T) {
	ctx := context.Background()
	client := dstest.Emulator(ctx, t)
	type entity struct{ Value int }
	if _, err := client.Put(ctx, datastore.IncompleteKey("kind", nil), &entity{123}); err != nil {
		t.Error(err)
	}
	var got []entity
	if _, err := client.GetAll(ctx, datastore.NewQuery("kind"), &got); err != nil {
		t.Error(err)
	}
	want := []entity{{123}}
	if diff := cmp.Diff(got, want); diff != "" {
		t.Error("-got +want", diff)
	}
}
