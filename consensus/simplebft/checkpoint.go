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
	s.broadcast(&Msg{&Msg_Checkpoint{c}})
}

func (s *SBFT) handleCheckpoint(c *Checkpoint, src uint64) {
	if c.Seq < s.cur.subject.Seq.Seq {
		// old message
		return
	}
	if c.Seq != s.cur.subject.Seq.Seq {
		log.Infof("checkpoint does not match expected subject %v, got %v", &s.cur.subject, c)
		return
	}
	if _, ok := s.cur.checkpoint[src]; ok {
		log.Infof("duplicate checkpoint for %d from %d", c.Seq, src)
	}
	s.cur.checkpoint[src] = c

	max := "_"
	sums := make(map[string][]uint64)
	for csrc, c := range s.cur.checkpoint {
		sum := fmt.Sprintf("%x", c.State)
		sums[sum] = append(sums[sum], csrc)

		if len(sums[sum]) >= s.noFaultyQuorum() {
			max = sum
		}
	}

	replicas, ok := sums[max]
	if !ok || len(sums[max]) != s.noFaultyQuorum() {
		return
	}
	c = s.cur.checkpoint[replicas[0]]
	// got a stable checkpoint

	if !reflect.DeepEqual(c.State, s.cur.state) {
		log.Fatalf("stable checkpoint %x does not match our state %x",
			c.State, s.cur.state)
		// NOT REACHED
	}

	s.maybeSendNextBatch()
	s.processBacklog()
}
