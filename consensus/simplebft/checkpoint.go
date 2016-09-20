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
)

func (s *SBFT) sendCheckpoint() {
	state := []byte("XXX")
	s.cur.state = state
	c := &Checkpoint{
		Seq:   s.seq.Seq,
		State: state,
	}
	cs := s.sign(c)
	s.broadcast(&Msg{&Msg_Checkpoint{cs}})
}

func (s *SBFT) handleCheckpoint(cs *Signed, src uint64) {
	if s.cur.checkpointDone {
		return
	}

	c := &Checkpoint{}
	err := s.checkSig(cs, src, c)
	if err != nil {
		return
	}

	if c.Seq < s.cur.subject.Seq.Seq {
		// old message
		return
	}
	// TODO should we always accept checkpoints?
	if c.Seq != s.cur.subject.Seq.Seq {
		log.Infof("checkpoint does not match expected subject %v, got %v", &s.cur.subject, c)
		return
	}
	if _, ok := s.cur.checkpoint[src]; ok {
		log.Infof("duplicate checkpoint for %d from %d", c.Seq, src)
	}
	s.cur.checkpoint[src] = c
	s.cur.checkpointRaw[src] = cs

	max := "_"
	sums := make(map[string][]uint64)
	for csrc, c := range s.cur.checkpoint {
		sum := fmt.Sprintf("%x", c.State)
		sums[sum] = append(sums[sum], csrc)

		if len(sums[sum]) >= s.oneCorrectQuorum() {
			max = sum
		}
	}

	replicas, ok := sums[max]
	if !ok || len(sums[max]) != s.oneCorrectQuorum() {
		return
	}

	// got a stable checkpoint

	cpset := &CheckpointSet{make(map[uint64]*Signed)}
	for _, r := range replicas {
		cpset.CheckpointSet[r] = s.cur.checkpointRaw[r]
	}
	s.sys.Persist("checkpoint", cpset)
	s.cur.checkpointDone = true

	c = s.cur.checkpoint[replicas[0]]

	if !reflect.DeepEqual(c.State, s.cur.state) {
		log.Fatalf("weak checkpoint %x does not match our state %x",
			c.State, s.cur.state)
		// NOT REACHED
	}

	// ignore null requests
	if s.cur.payload.Digest != nil {
		s.sys.Deliver(s.cur.subject.Seq.Seq, s.cur.payload.Digest, cpset)
	}

	s.maybeSendNextBatch()
	s.processBacklog()
}
