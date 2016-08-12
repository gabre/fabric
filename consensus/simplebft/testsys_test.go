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
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"time"
)

type testSystemAdapter struct {
	id      uint64
	sys     *testSystem
	batches [][][]byte
}

func (t *testSystemAdapter) Send(msg *Msg, dest uint64) {
	// TODO this is the place to emulate message latency
	inflight := 20 * time.Millisecond
	ev := &testMsgEvent{
		inflight: inflight,
		src:      t.id,
		dst:      dest,
		msg:      msg,
	}
	t.sys.enqueue(inflight, ev)
}

type testMsgEvent struct {
	inflight time.Duration
	src, dst uint64
	msg      *Msg
}

func (ev *testMsgEvent) Exec() {
	// XXX
}

func (ev *testMsgEvent) String() string {
	return fmt.Sprintf("Message<from %d, to %d, inflight %s, %v",
		ev.src, ev.dst, ev.inflight, ev.msg)
}

type testTimer struct {
	id        uint64
	tf        timerFunc
	cancelled bool
}

func (t *testTimer) Cancel() {
	t.cancelled = true
}

func (t *testTimer) Exec() {
	if !t.cancelled {
		t.tf()
	}
}

func (t *testTimer) String() string {
	fun := runtime.FuncForPC(reflect.ValueOf(t.tf).Pointer()).Name()
	return fmt.Sprintf("Timer<on %d, cancelled %v, fun %s>", t.id, t.cancelled, fun)
}

func (t *testSystemAdapter) Timer(d time.Duration, tf timerFunc) Canceller {
	tt := &testTimer{id: t.id, tf: tf}
	t.sys.enqueue(d, tt)
	return tt
}

func (t *testSystemAdapter) Deliver(batch [][]byte) {
	t.batches = append(t.batches, batch)
}

// ==============================================

type testEvent interface {
	Exec()
}

// ==============================================

type testSystem struct {
	now      time.Duration
	queue    []testElem
	adapters map[uint64]*testSystemAdapter
}

type testElem struct {
	at time.Duration
	ev testEvent
}

func (t testElem) String() string {
	return fmt.Sprintf("Event<%s: %s>", t.at, t.ev)
}

type testElemQueue []testElem

func (q testElemQueue) Len() int {
	return len(q)
}

func (q testElemQueue) Less(i, j int) bool {
	return q[i].at < q[j].at
}

func (q testElemQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func newTestSystem() *testSystem {
	return &testSystem{adapters: make(map[uint64]*testSystemAdapter)}
}

func (t *testSystem) NewAdapter(id uint64) *testSystemAdapter {
	a := &testSystemAdapter{
		id:  id,
		sys: t,
	}
	t.adapters[id] = a
	return a
}

func (t *testSystem) enqueue(d time.Duration, ev testEvent) {
	e := testElem{at: t.now + d, ev: ev}
	testLog.Debugf("enqueuing %s\n", e)
	t.queue = append(t.queue, e)
	sort.Sort(testElemQueue(t.queue))
}

func (t *testSystem) Run() {
	for len(t.queue) > 0 {
		var e testElem
		e, t.queue = t.queue[0], t.queue[1:]
		t.now = e.at
		testLog.Debugf("executing %s\n", e)
		e.ev.Exec()
	}
}
