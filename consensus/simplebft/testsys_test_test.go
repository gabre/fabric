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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
	"time"
)

func TestSys(t *testing.T) {
	s := newTestSystem(1)
	called := false
	s.enqueue(1*time.Second, &testTimer{tf: func() {
		called = true
	}})
	s.Run()
	if !called {
		t.Fatal("expected execution")
	}
}

// func TestMsg(t *testing.T) {
// 	s := newTestSystem()
// 	a := s.NewAdapter(0)
// 	called := false
// 	a.Send(nil, 0)
// 	s.enqueue(1*time.Second, &testTimer{tf: func() {
// 		called = true
// 	}})
// 	s.Run()
// 	if !called {
// 		t.Fatal("expected execution")
// 	}
// }

func BenchmarkEcdsa(b *testing.B) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	val := make([]byte, 32, 32)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ecdsa.Sign(rand.Reader, key, val)
	}
}
