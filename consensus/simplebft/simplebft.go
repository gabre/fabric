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
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/op/go-logging"
)

type Receiver interface {
	Receive(msg *Msg, src uint64)
}

type System interface {
	Send(msg *Msg, dest uint64)
	Timer(d time.Duration, t timerFunc) Canceller
	Deliver(batch [][]byte)
	SetReceiver(receiver Receiver)
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
	cur        reqInfo
}

type reqInfo struct {
	subject    Subject
	payload    *DigestSet
	preprep    *Preprepare
	prep       map[uint64]*Subject
	commit     map[uint64]*Subject
	sentCommit bool
	executed   bool
}

var log = logging.MustGetLogger("sbft")

func New(id uint64, config *Config, sys System) (*SBFT, error) {
	s := &SBFT{
		config: *config,
		sys:    sys,
		id:     id,
	}
	// XXX retrieve current seq
	s.seq.View = 0
	s.seq.Seq = 0
	s.sys.SetReceiver(s)
	return s, nil
}

func (s *SBFT) primaryId() uint64 {
	return s.seq.View % s.config.N
}

func (s *SBFT) isPrimary() bool {
	return s.primaryId() == s.id
}

func (s *SBFT) nextSeq() Seq {
	seq := s.seq
	seq.Seq += 1
	return seq
}

func (s *SBFT) noFaultyQuorum() int {
	return int(s.config.N - s.config.F)
}

func (s *SBFT) broadcast(m *Msg) {
	for i := uint64(0); i < s.config.N; i++ {
		s.sys.Send(m, i)
	}
}

func (s *SBFT) Request(req []byte) {
	s.broadcast(&Msg{&Msg_Request{&Request{req}}})
}

func (s *SBFT) Receive(m *Msg, src uint64) {
	if req := m.GetRequest(); req != nil {
		s.handleRequest(req, src)
	} else if pp := m.GetPreprepare(); pp != nil {
		s.handlePreprepare(pp, src)
	} else if p := m.GetPrepare(); p != nil {
		s.handlePrepare(p, src)
	} else if c := m.GetCommit(); c != nil {
		s.handleCommit(c, src)
	} else {
		log.Infof("received invalid message from %d", src)
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

func (s *SBFT) startBatchTimer() {
	if s.batchTimer == nil {
		s.batchTimer = s.sys.Timer(time.Duration(s.config.BatchDurationNsec), s.batchReady)
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
	seq := s.nextSeq()

	set := &DigestSet{Digest: make([][]byte, len(batch))}

	for i, req := range batch {
		set.Digest[i] = req.Payload
	}

	rawSet, err := proto.Marshal(set)
	if err != nil {
		panic(err)
	}

	m := &Preprepare{
		Seq: &seq,
		Set: rawSet,
	}

	s.broadcast(&Msg{&Msg_Preprepare{m}})
}

////////////////////////////////////////////////

func (s *SBFT) handlePreprepare(pp *Preprepare, src uint64) {
	if src != s.primaryId() {
		log.Debugf("preprepare from non-primary %d", src)
		return
	}
	nextSeq := s.nextSeq()
	if *pp.Seq != nextSeq {
		log.Noticef("preprepare does not match expected %v, got %v", nextSeq, *pp.Seq)
		return
	}
	s.seq = nextSeq
	h := s.hash(pp.Set)

	payload := &DigestSet{}
	err := proto.Unmarshal(pp.Set, payload)
	if err != nil {
		log.Noticef("preprepare digest set malformed in preprepare %v from %s, %s", s.seq, src, h)
	}

	s.cur = reqInfo{
		subject: Subject{Seq: &s.seq, Digest: h[:]},
		payload: payload,
		preprep: pp,
		prep:    make(map[uint64]*Subject),
		commit:  make(map[uint64]*Subject),
	}

	log.Infof("accepting preprepare for %v, %s", s.seq, h)
	if !s.isPrimary() {
		s.sendPrepare()
	}

	s.maybeSendCommit()
}

func (s *SBFT) sendPrepare() {
	p := s.cur.subject
	s.broadcast(&Msg{&Msg_Prepare{&p}})
}

////////////////////////////////////////////////

func (s *SBFT) handlePrepare(p *Subject, src uint64) {
	if !reflect.DeepEqual(p, &s.cur.subject) {
		log.Debugf("prepare does not match expected subject %v, got %v", &s.cur.subject, p)
		return
	}
	if _, ok := s.cur.prep[src]; ok {
		log.Debugf("duplicate prepare for %v from %d", *p.Seq, src)
		return
	}
	s.cur.prep[src] = p
	s.maybeSendCommit()
}

func (s *SBFT) maybeSendCommit() {
	if s.cur.sentCommit || len(s.cur.prep) < s.noFaultyQuorum()-1 {
		return
	}
	s.cur.sentCommit = true
	c := s.cur.subject
	s.broadcast(&Msg{&Msg_Commit{&c}})
}

////////////////////////////////////////////////

func (s *SBFT) handleCommit(c *Subject, src uint64) {
	if !reflect.DeepEqual(c, &s.cur.subject) {
		log.Debugf("commit does not match expected subject %v, got %v", &s.cur.subject, c)
		return
	}
	if _, ok := s.cur.commit[src]; ok {
		log.Debugf("duplicate commit for %v from %d", *c.Seq, src)
		return
	}
	s.cur.commit[src] = c
	s.maybeExecute()
}

func (s *SBFT) maybeExecute() {
	if s.cur.executed || len(s.cur.prep) < s.noFaultyQuorum() {
		return
	}
	s.cur.executed = true
	log.Noticef("executing %v", s.seq)
	s.seq = *s.cur.subject.Seq

	s.sys.Deliver(s.cur.payload.Digest)
}
