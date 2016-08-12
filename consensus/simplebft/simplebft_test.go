/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package simplebft

import (
	"testing"

	"github.com/op/go-logging"
)

var testLog = logging.MustGetLogger("test")

func init() {
	logging.SetBackend(logging.InitForTesting(logging.INFO))
}

func TestSBFT(t *testing.T) {
	sys := newTestSystem()
	s, err := New(0, &Config{N: 4, F: 1, BatchDurationNsec: 2000000000, BatchSizeBytes: 1}, sys.NewAdapter(0))
	if err != nil {
		t.Fatal(err)
	}
	s.Request([]byte{1, 2, 3})
	sys.Run()
}

func BenchmarkRequestPreprepare(b *testing.B) {
	sys := newTestSystem()
	s, _ := New(0, &Config{N: 2, F: 1, BatchDurationNsec: 2000000000, BatchSizeBytes: 1}, sys.NewAdapter(0))
	// for i := 0; i < b.N; i++ {
	// 	s.Request([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
	// }
	// sys.Run()
	for i := 0; i < b.N; i++ {
		s.Request([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
		sys.Run()
	}
}
