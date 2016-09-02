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

func (s *SBFT) maybeSendNewView() {
	if s.lastNewViewSent == s.seq.View {
		return
	}

	vset := make(map[uint64]*Signed)
	var vcs []*ViewChange

	for src, vc := range s.viewchange {
		if vc.vc.View == s.seq.View {
			vset[src] = vc.svc
			vcs = append(vcs, vc.vc)
		}
	}

	xset, ok := s.makeXset(vcs)
	if !ok {
		return
	}

	nv := &NewView{
		View: s.seq.View,
		Vset: vset,
		Xset: xset,
	}
	log.Noticef("sending new view for %d", nv.View)
	s.lastNewViewSent = nv.View
	s.broadcast(&Msg{&Msg_NewView{nv}})
}

func (s *SBFT) makeXset(vcs []*ViewChange) ([]*Subject, bool) {
	// first select base commit (equivalent to checkpoint/low water mark)
	// 1. need weak quorum
	quora := make(map[uint64]int)
	for _, vc := range vcs {
		quora[vc.Executed] += 1
	}
	best := uint64(0)
	found := false
	for seq, count := range quora {
		if count < s.oneCorrectQuorum() {
			continue
		}
		// 2. need 2f+1 from S below (or equal to) seq
		sum := 0
		for seq2, count2 := range quora {
			if seq2 <= seq {
				sum += count2
			}
		}
		if sum < s.noFaultyQuorum() {
			continue
		}
		found = true
		if seq > best {
			best = seq
		}
	}
	if !found {
		return nil, false
	}

	log.Debugf("xset starts at commit %d", best)

	// now determine which request could have executed for best+1
	next := best + 1
	var xset []*Subject

	// find some message m in S,
	emptycount := 0
nextm:
	for _, m := range vcs {
		notfound := true
		// which has <n,d,v> in its Pset
		for _, mtuple := range m.Pset {
			log.Debugf("trying %v", mtuple)
			if mtuple.Seq.Seq < next {
				continue
			}

			// we found an entry for next
			notfound = false

			// A1. where 2f+1 messages mp from S
			count := 0
		nextmp:
			for _, mp := range vcs {
				// "low watermark" is less than n
				if mp.Executed > mtuple.Seq.Seq {
					continue
				}
				// and all <n,d',v'> in its Pset
				for _, mptuple := range mp.Pset {
					log.Debugf("  matching %v", mptuple)
					if mptuple.Seq.Seq != mtuple.Seq.Seq {
						continue
					}

					// either v' < v or (v' == v and d' == d)
					if mptuple.Seq.View < mtuple.Seq.View ||
						(mptuple.Seq.View == mtuple.Seq.View && reflect.DeepEqual(mptuple.Digest, mtuple.Digest)) {
						continue
					} else {
						continue nextmp
					}
				}
				count += 1
			}
			if count < s.noFaultyQuorum() {
				continue
			}
			log.Debugf("found %d replicas for Pset %d/%d", count, mtuple.Seq.Seq, mtuple.Seq.View)

			// A2. f+1 messages mp from S
			count = 0
			for _, mp := range vcs {
				// and all <n,d',v'> in its Qset
				for _, mptuple := range mp.Qset {
					if mptuple.Seq.Seq != mtuple.Seq.Seq {
						continue
					}
					if mptuple.Seq.View < mtuple.Seq.View {
						continue
					}
					// d' == d
					if !reflect.DeepEqual(mptuple.Digest, mtuple.Digest) {
						continue
					}
					count += 1
					// there exists one ...
					break
				}
			}
			if count < s.oneCorrectQuorum() {
				continue
			}
			log.Debugf("found %d replicas for Qset %d", count, mtuple.Seq.Seq)

			log.Debugf("selecting %d with %x", next, mtuple.Digest)
			xset = append(xset, &Subject{
				Seq:    &Seq{Seq: next, View: s.seq.View},
				Digest: mtuple.Digest,
			})
			break nextm
		}

		if notfound {
			emptycount += 1
		}
	}
	if emptycount >= s.noFaultyQuorum() {
		log.Debugf("selecting null request for %d", next)
		xset = append(xset, &Subject{
			Seq:    &Seq{Seq: next, View: s.seq.View},
			Digest: nil,
		})
	}

	if len(xset) == 0 {
		return nil, false
	}

	return xset, true
}

func (s *SBFT) handleNewView(nv *NewView, src uint64) {
	if src != s.primaryIdView(nv.View) {
		log.Warningf("invalid new view from %d for %d", src, nv.View)
		return
	}

	if onv, ok := s.newview[s.primaryIdView(nv.View)]; ok && onv.View >= nv.View {
		log.Debugf("discarding duplicate new view for %d", nv.View)
		return
	}

	var vcs []*ViewChange
	for vcsrc, svc := range nv.Vset {
		vc := &ViewChange{}
		err := s.checkSig(svc, vcsrc, vc)
		if err == nil {
			if vc.View != nv.View {
				err = fmt.Errorf("view does not match")
			}
		}
		if err != nil {
			log.Warningf("invalid new view from %d: view change for %d: %s", src, vcsrc, err)
			return
		}
		vcs = append(vcs, vc)
	}

	if len(nv.Vset) != s.noFaultyQuorum() {
		log.Warningf("invalid new view from %d: wrong number of view change messages (%d)", src, len(nv.Vset))
		return
	}

	xset, ok := s.makeXset(vcs)
	if !ok || !reflect.DeepEqual(nv.Xset, xset) {
		log.Warningf("invaliud new view from %d: xset incorrect", src)
		return
	}

	s.newview[s.primaryIdView(nv.View)] = nv
}

func (s *SBFT) processNewView() {
	if s.activeView {
		return
	}

	nv, ok := s.newview[s.primaryIdView(s.seq.View)]
	if !ok || nv.View != s.seq.View {
		return
	}

	s.activeView = true

	if !s.isPrimary() {
	}
}
