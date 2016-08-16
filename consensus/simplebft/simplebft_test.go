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
	"reflect"
	"testing"

	"github.com/op/go-logging"
)

var testLog = logging.MustGetLogger("test")

func init() {
	logging.SetLevel(logging.NOTICE, "test")
}

func TestSBFT(t *testing.T) {
	sys := newTestSystem()
	var repls []*SBFT
	var adapters []*testSystemAdapter
	N := uint64(4)
	for i := uint64(0); i < N; i++ {
		a := sys.NewAdapter(i)
		s, err := New(i, &Config{N: N, F: 1, BatchDurationNsec: 2000000000, BatchSizeBytes: 10}, a)
		if err != nil {
			t.Fatal(err)
		}
		repls = append(repls, s)
		adapters = append(adapters, a)
	}
	r1 := []byte{1, 2, 3}
	repls[0].Request(r1)
	sys.Run()
	r2 := []byte{3, 1, 2}
	r3 := []byte{3, 5, 2}
	repls[1].Request(r2)
	repls[1].Request(r3)
	sys.Run()
	for _, a := range adapters {
		if len(a.batches) != 2 {
			t.Error("expected execution of 2 batches")
		}
		if !reflect.DeepEqual([][]byte{r1}, a.batches[0]) {
			t.Error("wrong request executed (1)")
		}
		if !reflect.DeepEqual([][]byte{r2, r3}, a.batches[1]) {
			t.Error("wrong request executed (2)")
		}
	}
}

func BenchmarkRequestPreprepare(b *testing.B) {
	logging.SetLevel(logging.NOTICE, "")
	sys := newTestSystem()
	s, _ := New(0, &Config{N: 1, F: 0, BatchDurationNsec: 2000000000, BatchSizeBytes: 1}, sys.NewAdapter(0))
	for i := 0; i < b.N; i++ {
		s.Request([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
		sys.Run()
	}
	logging.SetLevel(logging.DEBUG, "")
}
