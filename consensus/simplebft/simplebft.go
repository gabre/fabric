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
	"time"

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

	config            Config
	id                uint64
	seq               Seq
	batch             []*Request
	batchTimer        Canceller
	cur               reqInfo
	activeView        bool
	viewchange        map[uint64]*viewChangeInfo
	newview           map[uint64]*NewView
	lastNewViewSent   uint64
	viewChangeTimeout time.Duration
	viewChangeTimer   Canceller
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

type dummyCanceller struct{}

func (d dummyCanceller) Cancel() {}

func New(id uint64, config *Config, sys System) (*SBFT, error) {
	if config.F*3+1 > config.N {
		return nil, fmt.Errorf("invalid combination of N and F")
	}

	s := &SBFT{
		config:          *config,
		sys:             sys,
		id:              id,
		viewchange:      make(map[uint64]*viewChangeInfo),
		newview:         make(map[uint64]*NewView),
		viewChangeTimer: dummyCanceller{},
	}
	s.sys.SetReceiver(s)
	// XXX retrieve current seq
	s.seq.View = 0
	s.seq.Seq = 0
	// XXX set active after checking with the network
	s.activeView = true
	s.cur.executed = true

	s.cancelViewChangeTimer()
	return s, nil
}

////////////////////////////////////////////////

func (s *SBFT) primaryIdView(v uint64) uint64 {
	return v % s.config.N
}

func (s *SBFT) primaryId() uint64 {
	return s.primaryIdView(s.seq.View)
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

func (s *SBFT) broadcast(m *Msg) {
	for i := uint64(0); i < s.config.N; i++ {
		s.sys.Send(m, i)
	}
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
