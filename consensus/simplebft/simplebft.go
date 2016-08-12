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
	"time"

	"github.com/golang/protobuf/proto"
)

type System interface {
	Send(msg *Msg, dest uint64)
	Timer(d time.Duration, t timerFunc) Canceller
	Deliver(batch [][]byte)
}

type timerFunc func()

type Canceller interface {
	Cancel()
}

type SBFT struct {
	sys System

	config     Config
	id         uint64
	seq        Seq
	batch      []*Request
	batchTimer Canceller
}

type msg struct {
	msg *Msg
	src uint64
}

func New(id uint64, config *Config, sys System) (*SBFT, error) {
	s := &SBFT{
		config: *config,
		sys:    sys,
		id:     id,
	}
	// XXX retrieve current seq
	s.seq.View = 0
	s.seq.Seq = 0
	return s, nil
}

func (s *SBFT) primaryId() uint64 {
	return s.seq.View % s.config.N
}

func (s *SBFT) isPrimary() bool {
	return s.primaryId() == s.id
}

func (s *SBFT) broadcast(m *Msg) {
	for i := uint64(0); i < s.config.N; i++ {
		if i != s.id {
			s.sys.Send(m, i)
		}
	}
	s.Receive(m, s.id)
}

func (s *SBFT) Request(req []byte) {
	s.broadcast(&Msg{&Msg_Request{&Request{req}}})
}

func (s *SBFT) Receive(m *Msg, src uint64) {
	if req := m.GetRequest(); req != nil {
		s.handleRequest(req, src)
	}
}

////////////////////////////////////////////////

func (s *SBFT) handleRequest(req *Request, src uint64) {
	if s.isPrimary() {
		s.batch = append(s.batch, req)
		if s.batchSize() >= s.config.BatchSizeBytes {
			s.batchReady()
		} else {
			s.startBatchTimer()
		}
	}
}

func (s *SBFT) batchSize() uint64 {
	size := uint64(0)
	for _, req := range s.batch {
		size += uint64(len(req.Payload))
	}
	return size
}

func (s *SBFT) batchReady() {
	if s.batchTimer != nil {
		s.batchTimer.Cancel()
		s.batchTimer = nil
	}

	batch := s.batch
	s.batch = nil
	s.sendPreprepare(batch)
}

func (s *SBFT) sendPreprepare(batch []*Request) {
	s.seq.Seq += 1

	set := &DigestSet{Digest: make([][]byte, len(batch))}

	for i, req := range batch {
		set.Digest[i] = req.Payload
	}

	rawSet, err := proto.Marshal(set)
	if err != nil {
		panic(err)
	}

	seq := s.seq
	m := &Preprepare{
		Seq: &seq,
		Set: rawSet,
	}

	s.broadcast(&Msg{&Msg_Preprepare{m}})
}

func (s *SBFT) startBatchTimer() {
	if s.batchTimer == nil {
		s.batchTimer = s.sys.Timer(time.Duration(s.config.BatchDurationNsec), s.batchReady)
	}
}
