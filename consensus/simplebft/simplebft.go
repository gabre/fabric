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
	activeView bool
	viewchange map[uint64]*viewChangeInfo
}

type reqInfo struct {
	subject    Subject
	timeout    Canceller
	payload    *DigestSet
	preprep    *Preprepare
	prep       map[uint64]*Subject
	commit     map[uint64]*Subject
	sentCommit bool
	executed   bool
	state      []byte
	checkpoint map[uint64]*Checkpoint
}

type viewChangeInfo struct {
	svc *Signed
	vc  *ViewChange
}

var log = logging.MustGetLogger("sbft")

func New(id uint64, config *Config, sys System) (*SBFT, error) {
	if config.F*3+1 > config.N {
		return nil, fmt.Errorf("invalid combination of N and F")
	}

	s := &SBFT{
		config:     *config,
		sys:        sys,
		id:         id,
		viewchange: make(map[uint64]*viewChangeInfo),
	}
	s.sys.SetReceiver(s)
	// XXX retrieve current seq
	s.seq.View = 0
	s.seq.Seq = 0
	// XXX set active after checking with the network
	s.activeView = true
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

func (s *SBFT) nextView() uint64 {
	return s.seq.View + 1
}

func (s *SBFT) noFaultyQuorum() int {
	return int(s.config.N - s.config.F)
}

func (s *SBFT) oneCorrectQuorum() int {
	return int(s.config.F + 1)
}

////////////////////////////////////////////////

func (s *SBFT) broadcast(m *Msg) {
	for i := uint64(0); i < s.config.N; i++ {
		s.sys.Send(m, i)
	}
}

func (s *SBFT) Request(req []byte) {
	s.broadcast(&Msg{&Msg_Request{&Request{req}}})
}

////////////////////////////////////////////////

func (s *SBFT) Receive(m *Msg, src uint64) {
	log.Debugf("received message from %d: %s", src, m)
	if req := m.GetRequest(); req != nil {
		s.handleRequest(req, src)
		return
	} else if vs := m.GetViewChange(); vs != nil {
		s.handleViewChange(vs, src)
		return
	} else if c := m.GetCheckpoint(); c != nil {
		s.handleCheckpoint(c, src)
		return
	} else if nv := m.GetNewView(); nv != nil {
		s.handleNewView(nv, src)
		return
	}

	if !s.activeView {
		log.Infof("we are not active in view %d, discarding message from %d",
			s.seq.View, src)
		return
	}

	if pp := m.GetPreprepare(); pp != nil {
		s.handlePreprepare(pp, src)
		return
	} else if p := m.GetPrepare(); p != nil {
		s.handlePrepare(p, src)
		return
	} else if c := m.GetCommit(); c != nil {
		s.handleCommit(c, src)
		return
	}

	log.Warningf("received invalid message from %d", src)
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
		log.Infof("preprepare from non-primary %d", src)
		return
	}
	nextSeq := s.nextSeq()
	if *pp.Seq != nextSeq {
		log.Infof("preprepare does not match expected %v, got %v", nextSeq, *pp.Seq)
		return
	}
	s.seq = nextSeq
	h := s.hash(pp.Set)

	payload := &DigestSet{}
	err := proto.Unmarshal(pp.Set, payload)
	if err != nil {
		log.Infof("preprepare digest set malformed in preprepare %v from %s, %s", s.seq, src, h)
		return
	}

	s.cur = reqInfo{
		subject:    Subject{Seq: &s.seq, Digest: h[:]},
		timeout:    s.sys.Timer(time.Duration(s.config.RequestTimeoutNsec)*time.Nanosecond, s.requestTimeout),
		payload:    payload,
		preprep:    pp,
		prep:       make(map[uint64]*Subject),
		commit:     make(map[uint64]*Subject),
		checkpoint: make(map[uint64]*Checkpoint),
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
		log.Infof("prepare does not match expected subject %v, got %v", &s.cur.subject, p)
		return
	}
	if _, ok := s.cur.prep[src]; ok {
		log.Infof("duplicate prepare for %v from %d", *p.Seq, src)
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
		log.Infof("commit does not match expected subject %v, got %v", &s.cur.subject, c)
		return
	}
	if _, ok := s.cur.commit[src]; ok {
		log.Infof("duplicate commit for %v from %d", *c.Seq, src)
		return
	}
	s.cur.commit[src] = c
	s.maybeExecute()
}

func (s *SBFT) maybeExecute() {
	if s.cur.executed || len(s.cur.commit) < s.noFaultyQuorum() {
		return
	}
	s.cur.executed = true
	s.cur.timeout.Cancel()
	s.seq = *s.cur.subject.Seq
	log.Noticef("executing %v", s.seq)

	s.sys.Deliver(s.cur.payload.Digest)

	s.sendCheckpoint()
}

////////////////////////////////////////////////

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
	if c.Seq != s.cur.subject.Seq.Seq {
		log.Infof("commit does not match expected subject %v, got %v", &s.cur.subject, c)
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
		log.Fatalf("stable checkpoint does not match our state")
		// NOT REACHED
	}
}

////////////////////////////////////////////////

func (s *SBFT) requestTimeout() {
	log.Infof("request timed out: %s", s.cur.subject.Seq)
	s.sendViewChange()
}

func (s *SBFT) sendViewChange() {
	s.seq.View = s.nextView()
	s.cur.timeout.Cancel()
	s.activeView = false
	for r, vs := range s.viewchange {
		if vs.vc.View < s.seq.View {
			delete(s.viewchange, r)
		}
	}
	log.Noticef("sending viewchange for view %d", s.seq.View)

	var q, p *Subject
	if s.cur.sentCommit {
		p = &s.cur.subject
	} else if s.cur.preprep != nil {
		q = &s.cur.subject
	}

	vc := &ViewChange{
		View: s.seq.View,
		Qset: q,
		Pset: p,
	}
	svc := s.sign(vc)
	s.broadcast(&Msg{&Msg_ViewChange{svc}})
}

////////////////////////////////////////////////

func (s *SBFT) handleViewChange(svc *Signed, src uint64) {
	vc := &ViewChange{}
	err := s.checkSig(svc, src, vc)
	if err != nil {
		log.Noticef("invalid viewchange: %s", err)
		return
	}
	if vc.View < s.seq.View {
		log.Debugf("old view change from %s for view %d, we are in view %d", src, vc.View, s.seq.View)
		return
	}
	if ovc, ok := s.viewchange[src]; ok && vc.View <= ovc.vc.View {
		log.Noticef("duplicate view change for %d from %d", vc.View, src)
		return
	}

	log.Infof("viewchange from %d for view %d", src, vc.View)
	s.viewchange[src] = &viewChangeInfo{svc: svc, vc: vc}

	if len(s.viewchange) == s.oneCorrectQuorum() {
		min := vc.View
		for _, vc := range s.viewchange {
			if vc.vc.View < min {
				min = vc.vc.View
			}
		}
		// catch up to the minimum view
		if s.seq.View < min {
			s.seq.View = min - 1
			s.sendViewChange()
			return
		}
	}

	if len(s.viewchange) != s.noFaultyQuorum() {
		log.Debug("not acting on viewchange: have %d", len(s.viewchange))
		return
	}

	if s.isPrimary() {
		s.sendNewView()
	}
}

func (s *SBFT) sendNewView() {
	var vset []*Signed

	for _, vc := range s.viewchange {
		vset = append(vset, vc.svc)
	}

	nv := &NewView{
		View: s.seq.View,
		Vset: vset,
	}
	log.Noticef("sending new view for %d", nv.View)
	s.broadcast(&Msg{&Msg_NewView{nv}})
}

////////////////////////////////////////////////

func (s *SBFT) handleNewView(nv *NewView, src uint64) {
}
