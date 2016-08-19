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

func (s *SBFT) maybeSendNewView() {
	var vset []*Signed

	for _, vc := range s.viewchange {
		if vc.vc.View == s.seq.View {
			vset = append(vset, vc.svc)
		}
	}

	if len(vset) < s.noFaultyQuorum() {
		return
	}

	nv := &NewView{
		View: s.seq.View,
		Vset: vset,
	}
	log.Noticef("sending new view for %d", nv.View)
	s.broadcast(&Msg{&Msg_NewView{nv}})
}

func (s *SBFT) handleNewView(nv *NewView, src uint64) {
}
