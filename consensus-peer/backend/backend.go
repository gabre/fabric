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

package backend

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/transport"

	"crypto/ecdsa"
	crand "crypto/rand"
	"math/big"

	"golang.org/x/net/context"

	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/asn1"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/consensus-peer/connection"
	"github.com/hyperledger/fabric/consensus-peer/persist"
	s "github.com/hyperledger/fabric/consensus/simplebft"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("backend")

type Backend struct {
	consensus s.Receiver
	conn      *connection.Manager

	lock  sync.Mutex
	peers map[uint64]chan<- *s.Msg

	self     *PeerInfo
	peerInfo map[string]*PeerInfo

	queue chan Executable

	receiver    s.Receiver
	persistence *persist.Persist
}

type Timer struct {
	tf      func()
	execute bool
}

func (t *Timer) Cancel() {
	t.execute = false
}

type Executable interface {
	Execute(*Backend)
}

func (t *Timer) Execute(backend *Backend) {
	if t.execute {
		t.tf()
	}
}

type msgEvent struct {
	msg *s.Msg
	src uint64
}

func (m *msgEvent) Execute(backend *Backend) {
	backend.consensus.Receive(m.msg, m.src)
}

type consensusConn Backend

type PeerInfo struct {
	info connection.PeerInfo
	id   uint64
}

type peerInfoSlice []*PeerInfo

func (pi peerInfoSlice) Len() int {
	return len(pi)
}

func (pi peerInfoSlice) Less(i, j int) bool {
	return strings.Compare(pi[i].info.Fingerprint(), pi[j].info.Fingerprint()) == -1
}

func (pi peerInfoSlice) Swap(i, j int) {
	pi[i], pi[j] = pi[j], pi[i]
}

func NewBackend(peers map[string][]byte, conn *connection.Manager, persist *persist.Persist) (*Backend, error) {
	c := &Backend{
		conn:     conn,
		peers:    make(map[uint64]chan<- *s.Msg),
		peerInfo: make(map[string]*PeerInfo),
	}

	var peerInfo []*PeerInfo
	for addr, cert := range peers {
		pi, err := connection.NewPeerInfo(addr, cert)
		if err != nil {
			return nil, err
		}
		cpi := &PeerInfo{info: pi}
		if pi.Fingerprint() == conn.Self.Fingerprint() {
			c.self = cpi
		}
		peerInfo = append(peerInfo, cpi)
		c.peerInfo[pi.Fingerprint()] = cpi
	}

	sort.Sort(peerInfoSlice(peerInfo))
	for i, pi := range peerInfo {
		pi.id = uint64(i)
		logger.Infof("replica %d: %s", i, pi.info.Fingerprint())
	}

	if c.self == nil {
		return nil, fmt.Errorf("peer list does not contain local node")
	}

	logger.Infof("we are replica %d (%s)", c.self.id, c.self.info)

	for _, peer := range c.peerInfo {
		if peer == c.self {
			continue
		}
		go c.connectWorker(peer)
	}
	s.RegisterConsensusServer(conn.Server, (*consensusConn)(c))
	c.persistence = persist
	go c.run()
	return c, nil
}

func (c *Backend) RegisterConsenter(consensus s.Receiver) {
	c.consensus = consensus
}

func (c *Backend) connectWorker(peer *PeerInfo) {
	timeout := 1 * time.Second

	delay := time.After(0)
	for {
		// pace reconnect attempts
		<-delay

		// set up for next
		delay = time.After(timeout)

		logger.Infof("connecting to replica %d (%s)", peer.id, peer.info)
		conn, err := c.conn.DialPeer(peer.info, grpc.WithBlock(), grpc.WithTimeout(timeout))
		if err != nil {
			logger.Warningf("could not connect to replica %d (%s): %s", peer.id, peer.info, err)
			continue
		}

		ctx := context.TODO()

		client := s.NewConsensusClient(conn)
		consensus, err := client.Consensus(ctx, &s.Handshake{})
		if err != nil {
			logger.Warningf("could not establish consensus stream with replica %d (%s): %s", peer.id, peer.info, err)
			continue
		}

		logger.Noticef("connection to replica %d (%s) established", peer.id, peer.info)

		for {
			msg, err := consensus.Recv()
			if err == io.EOF || err == transport.ErrConnClosing {
				break
			}
			if err != nil {
				logger.Warningf("consensus stream with replica %d (%s) broke: %v", peer.id, peer.info, err)
				break
			}
			c.enqueueForReceive(msg, peer.id)
		}
	}
}

func (b *Backend) enqueueForReceive(msg *s.Msg, src uint64) {
	b.queue <- &msgEvent{msg: msg, src: src}
}

func (b *Backend) initTimer(t *Timer, d time.Duration) {
	send := func() {
		if t.execute {
			b.queue <- t
		}
	}
	time.AfterFunc(d, send)
}

func (b *Backend) run() {
	for {
		e := <-b.queue
		e.Execute(b)
	}
}

// gRPC interface
func (c *consensusConn) Consensus(_ *s.Handshake, srv s.Consensus_ConsensusServer) error {
	pi := connection.GetPeerInfo(srv)
	peer, ok := c.peerInfo[pi.Fingerprint()]

	if !ok || !peer.info.Cert().Equal(pi.Cert()) {
		logger.Infof("rejecting connection from unknown replica %s", pi)
		return fmt.Errorf("unknown peer certificate")
	}
	logger.Infof("connection from replica %d (%s)", peer.id, pi)

	ch := make(chan *s.Msg)
	c.lock.Lock()
	if oldch, ok := c.peers[peer.id]; ok {
		logger.Debugf("replacing connection from replica %d", peer.id)
		close(oldch)
	}
	c.peers[peer.id] = ch
	c.lock.Unlock()

	var err error
	for msg := range ch {
		err = srv.Send(msg)
		if err != nil {
			c.lock.Lock()
			delete(c.peers, peer.id)
			c.lock.Unlock()

			logger.Infof("lost connection from replica %d (%s): %s", peer.id, pi, err)
		}
	}

	return err
}

func (c *Backend) Broadcast(msg *s.Msg) error {
	c.lock.Lock()
	for _, ch := range c.peers {
		ch <- msg
	}
	c.lock.Unlock()
	return nil
}

func (c *Backend) Unicast(msg *s.Msg, dest uint64) error {
	c.lock.Lock()
	ch, ok := c.peers[dest]
	c.lock.Unlock()

	if !ok {
		err := fmt.Errorf("peer not found: %v", dest)
		logger.Debug(err)
		return err
	}
	ch <- msg
	return nil
}

func (t *Backend) SetReceiver(recv s.Receiver) {
	t.receiver = recv
}

func (t *Backend) Send(msg *s.Msg, dest uint64) {
	t.Unicast(msg, dest)
}

func (t *Backend) Timer(d time.Duration, tf func()) s.Canceller {
	tm := &Timer{tf: tf, execute: true}
	t.initTimer(tm, d)
	return tm
}

func (t *Backend) Deliver(batch *s.Batch) {
	// XXX when something needs to be written to the ledger
	// ledger.store
}

func (t *Backend) Persist(key string, data proto.Message) {
	if data == nil {
		t.persistence.DelState(key)
	} else {
		bytes, err := proto.Marshal(data)
		if err != nil {
			panic(err)
		}
		t.persistence.StoreState(key, bytes)
	}
}

func (t *Backend) Restore(key string, out proto.Message) bool {
	val, err := t.persistence.ReadState(key)
	if err != nil {
		return false
	}
	err = proto.Unmarshal(val, out)
	return (err == nil)
}

func (t *Backend) Sign(data []byte) []byte {
	return Sign(t.conn.Cert.PrivateKey, data)
}

func (t *Backend) CheckSig(data []byte, src uint64, sig []byte) error {
	return CheckSig(t.conn.Cert.Leaf.PublicKey, data, sig)
}

func Sign(privateKey crypto.PrivateKey, data []byte) []byte {
	var err error
	var encsig []byte
	hash := sha256.Sum256(data)
	switch pvk := privateKey.(type) {
	case *rsa.PrivateKey:
		encsig, err = pvk.Sign(crand.Reader, hash[:], crypto.SHA256)
		if err != nil {
			panic(err)
		}
	case *ecdsa.PrivateKey:
		r, s, err := ecdsa.Sign(crand.Reader, pvk, hash[:])
		if err != nil {
			panic(err)
		}
		encsig, err = asn1.Marshal(struct{ R, S *big.Int }{r, s})
	default:
		panic("Unsupported private key type given.")
	}
	if err != nil {
		panic(err)
	}
	return encsig
}

func CheckSig(publicKey crypto.PublicKey, data []byte, sig []byte) error {
	hash := sha256.Sum256(data)
	switch p := publicKey.(type) {
	case *ecdsa.PublicKey:
		s := struct{ R, S *big.Int }{}
		rest, err := asn1.Unmarshal(sig, &s)
		if err != nil {
			return err
		}
		if len(rest) != 0 {
			return fmt.Errorf("invalid signature (problem with asn unmarshalling for ECDSA)")
		}
		ok := ecdsa.Verify(p, hash[:], s.R, s.S)
		if !ok {
			return fmt.Errorf("invalid signature (problem with verification)")
		}
		return nil
	case *rsa.PublicKey:
		err := rsa.VerifyPKCS1v15(p, crypto.SHA256, hash[:], sig)
		return err
	default:
		return fmt.Errorf("Unsupported public key type.")
	}
}
