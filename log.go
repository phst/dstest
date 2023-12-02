// Copyright 2023 Philipp Stephani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dstest

import (
	"testing"

	"github.com/hashicorp/go-retryablehttp"
)

type logger struct{ t testing.TB }

func (l logger) Printf(msg string, args ...any) {
	l.t.Logf(msg, args...)
}

var _ retryablehttp.Logger = logger{}
