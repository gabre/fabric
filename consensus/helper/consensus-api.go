package helper
	
import (
	    context "golang.org/x/net/context"
    	pb "github.com/hyperledger/fabric/protos"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/util"
	    "fmt"
	    "time"
        "sync"
)

// Consensus API
type consensusAPI struct {
    engine *EngineImpl
    consToSend chan *pb.Payload
    subscribers chan *pb.ConsensusApi_GetConsensusStreamServer
    streamList *streamList
}

var c *consensusAPI

// NewConsensusAPIServer creates and returns a new Consensus API server instance.
func NewConsensusAPIServer() *consensusAPI {
	if c == nil {
        	c = new(consensusAPI)
        	c.engine = getEngineImpl()
        	c.consToSend = make(chan *pb.Payload)
        	c.subscribers = make(chan *pb.ConsensusApi_GetConsensusStreamServer)
        	c.streamList = newStreamList()
            return c
    	}
    	panic("Consensus API is a singleton. It must be instantiated only once.")
}

// ConsentData sends data to the consensus to achieve an agreement with other nodes.
func (c *consensusAPI) ConsentData(ctx context.Context, payload *pb.Payload) (*pb.Dummy, error) {
    logger.Info("Must consent data: %s", payload.Payload)
    if c.engine != nil {
        tx := &pb.Transaction{Type: pb.Transaction_CHAINCODE_INVOKE, Payload: payload.Payload, Uuid: "temporaryID"}
        data, err := proto.Marshal(tx)
        if err != nil {
            return &pb.Dummy{Success: false}, fmt.Errorf("Problem when creating a transaction")
        }
        msg := &pb.Message{Type: pb.Message_CHAIN_TRANSACTION, Payload: data, Timestamp: util.CreateUtcTimestamp()}
        engine.ProcessTransactionMsg(msg, tx)
        return &pb.Dummy{Success: true}, nil
    } else {
        // this part comes from core.peer.peer.go
        // in case it has no engine then it is a non-validating peer
        // this implies that it has to connect to other, validating peers
        // this part is now done in core.peer
        // - do we need to pull all these parts into consensus?
        // - how to separate consensus from core?
        // conn, err := NewPeerClientConnectionWithAddress(peerAddress)
        // if err != nil {
        //    return &pb.Response{Status: pb.Response_FAILURE, Msg: []byte(fmt.Sprintf("Error creating client to peer address=%s:  %s", peerAddress, err))}
        // }
        // defer conn.Close()
        // serverClient := pb.NewPeerClient(conn)
        // peerLogger.Debugf("Sending TX to Peer: %s", peerAddress)
        // response, err = serverClient.ProcessTransaction(context.Background(), transaction)
        // if err != nil {
        //     return &pb.Response{Status: pb.Response_FAILURE, Msg: []byte(fmt.Sprintf("Error calling ProcessTransaction on remote peer at address=%s:  %s", peerAddress, err))}
        // }
        // return response
        panic("Non-validator node contacted for consensus.")
        return nil, fmt.Errorf("This node is not a validator. Cannot consent data.")
    }
}

// GetConsensusStream returns a stream on client side, on server side, it is put into an array of stream
func (c *consensusAPI) GetConsensusStream(stream pb.ConsensusApi_GetConsensusStreamServer) error {
    	logger.Info("Getting a consensus stream")
    c.streamList.add(stream)
    L: for {
		select {
		case <-time.After(time.Second):
			_, err := stream.Recv()
			logger.Warning("There is something with channel: ", err)
                	if err != nil {
				logger.Error("Error with channel: ", err)
                c.streamList.del(stream)
                break L
			}
		}
	}
    	logger.Info("Ending a consensus stream")
    
        return nil
}

// SendNewConsensusToClients sends a signal of a newly made consensus to the observing clients.
func SendNewConsensusToClients(consensus []byte) error {
    logger.Info("Sending to clients: %s", consensus)
    pl := &pb.Payload{Payload:consensus, Proof:nil}
    for stream, _ := range c.streamList.m {
        err := stream.Send(pl)
        if nil != err {
            logger.Error("Problem with sending to %s", stream)
        }
    }
    return nil
}

type streamList struct {
    m map[pb.ConsensusApi_GetConsensusStreamServer]bool
    sync.RWMutex
}

func newStreamList() *streamList {
    s := new(streamList)
    s.m = make(map[pb.ConsensusApi_GetConsensusStreamServer]bool)
    return s
}

func (s *streamList) add(k pb.ConsensusApi_GetConsensusStreamServer) {
    s.Lock()
    defer s.Unlock()
    s.m[k] = true
}

func (s *streamList) del(k pb.ConsensusApi_GetConsensusStreamServer) {
    s.Lock()
    defer s.Unlock()
    delete(s.m, k)
}
