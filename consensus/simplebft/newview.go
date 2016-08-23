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
	vset := make(map[uint64]*Signed)
	var vcs []*ViewChange

	for src, vc := range s.viewchange {
		if vc.vc.View == s.seq.View {
			vset[src] = vc.svc
			vcs = append(vcs, vc.vc)
		}
	}

	if len(vset) < s.noFaultyQuorum() {
		return
	}

	nv := &NewView{
		View: s.seq.View,
		Vset: vset,
		Xset: s.makeXset(vcs),
	}
	log.Noticef("sending new view for %d", nv.View)
	s.broadcast(&Msg{&Msg_NewView{nv}})
}

func (s *SBFT) makeXset(vcs []*ViewChange) []*Subject {
	return nil
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

	if !reflect.DeepEqual(nv.Xset, s.makeXset(vcs)) {
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
