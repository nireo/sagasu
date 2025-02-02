package registry

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

var (
	ErrNotLeader = errors.New("not leader")
)

// Transport handles communication between Raft nodes.
type Transport struct {
	ln        net.Listener
	serverTls *tls.Config
	peerTls   *tls.Config
}

// NewTransport creates a new Transport.
func NewTransport(ln net.Listener) *Transport {
	return &Transport{
		ln: ln,
	}
}

// NewTLSTransport creates a new Transport with TLS enabled.
func NewTLSTransport(ln net.Listener, serverTls, peerTls *tls.Config) *Transport {
	return &Transport{
		ln:        ln,
		serverTls: serverTls,
		peerTls:   peerTls,
	}
}

// Dial connects to a remote Raft node appends the Raft RPC to the connection. This is mainly used
// to properly identify the connection as a Raft connection.
func (tn *Transport) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	if _, err = conn.Write([]byte{byte(1)}); err != nil {
		return nil, err
	}

	if tn.peerTls != nil {
		return tls.Client(conn, tn.peerTls), nil
	}

	return conn, nil
}

// Accept accepts a new connection from a remote Raft node.
func (tn *Transport) Accept() (net.Conn, error) {
	conn, err := tn.ln.Accept()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1)
	if _, err := conn.Read(b); err != nil {
		return nil, err
	}

	if b[0] != RaftRpc {
		return nil, fmt.Errorf("invalid Raft RPC")
	}

	if tn.serverTls != nil {
		return tls.Server(conn, tn.serverTls), nil
	}

	return conn, nil
}

// Close closes the transport.
func (tn *Transport) Close() error {
	return tn.ln.Close()
}

// Addr returns the address of the transport.
func (tn *Transport) Addr() net.Addr {
	return tn.ln.Addr()
}

// RaftRpc is the Raft RPC byte which is used to identify Raft connections.
const RaftRpc = 1

// Config is the configuration for the Raft store.
type Config struct {
	Raft struct {
		raft.Config
		Bootstrap         bool          // Bootstrap is used to determine if the Raft cluster should be bootstrapped
		SnapshotThreshold uint64        // Snapshot threshold for Raft signifies how many logs to keep before creating a snapshot
		SnapshotInterval  time.Duration // Snapshot interval for Raft signifies how often to create a snapshot
		DataDir           string        // DataDir is the directory to store the Raft data
	}
	Transport *Transport // Transport is the transport layer for Raft
}

// Store is the Raft store for the registry
type Store struct {
	config  *Config
	raft    *raft.Raft
	log     *zap.Logger
	tn      *raft.NetworkTransport
	storage *PersistantStorage
	state   *State
	lbMu    sync.RWMutex
	lbs     map[string]LoadBalancer // group name -> load balancer
}

// snapshot is a snapshot of the registry state.
type snapshot struct {
	created time.Time // the time the snapshot was created
	encoded []byte    // the registry state encoded as json
}

type getStateResponse struct {
	State *State
	Error error
}

type addToGroupResponse struct {
	Error error
}

type removeFromGroupResponse struct {
	Error error
}

func (s *Store) setupStorage() error {
	storage, err := NewStorage(s.config.Raft.DataDir)
	if err != nil {
		return err
	}
	s.storage = storage
	return nil
}

func (s *Store) setupState() error {
	state, err := s.storage.GetState()
	if err != nil {
		return err
	}

	s.state = state
	return nil
}

func NewStore(config *Config) (*Store, error) {
	store := &Store{
		config: config,
		state: &State{
			Services: make(map[string]*Group),
			Version:  0,
		},
		log: zap.L().With(zap.String("component", "raft")),
	}

	if err := store.setupStorage(); err != nil {
		return nil, err
	}

	if err := store.setupRaft(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *Store) setupRaft() error {
	if err := os.MkdirAll(filepath.Join(s.config.Raft.DataDir, "raft"), 0755); err != nil {
		return err
	}

	stablepb, err := raftboltdb.NewBoltStore(filepath.Join(s.config.Raft.DataDir, "raft", "raft.db"))
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(s.config.Raft.DataDir, "raft"), 3, os.Stdout)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	s.tn = raft.NewNetworkTransport(s.config.Transport, maxPool, timeout, os.Stdout)

	config := raft.DefaultConfig()
	config.SnapshotInterval = s.config.Raft.SnapshotInterval
	config.SnapshotThreshold = s.config.Raft.SnapshotThreshold

	if s.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = s.config.Raft.HeartbeatTimeout
	}

	if s.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = s.config.Raft.ElectionTimeout
	}

	if s.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = s.config.Raft.LeaderLeaseTimeout
	}

	if s.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = s.config.Raft.CommitTimeout
	}

	config.LocalID = raft.ServerID(s.config.Raft.LocalID)

	s.raft, err = raft.NewRaft(config, s, stablepb, stablepb, snapshots, s.tn)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(stablepb, stablepb, snapshots)
	if err != nil {
		return err
	}

	if s.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: raft.ServerAddress(s.config.Transport.Addr().String()),
			}},
		}
		err = s.raft.BootstrapCluster(config).Error()
	}
	return err
}

// Apply is used to apply a log to the registry
func (s *Store) Apply(log *raft.Log) interface{} {
	var req ApplyRequest
	if err := json.Unmarshal(log.Data, &req); err != nil {
		return err
	}

	if req.ActionType == "add" {
		if _, ok := s.state.Services[req.AddData.Group]; !ok {
			s.state.Services[req.AddData.Group] = &Group{
				Instances: make(map[string]*Instance),
			}
		}

		s.state.Services[req.AddData.Group].Instances[req.AddData.Instance.ID] = &req.AddData.Instance
		err := s.storage.SaveState(s.state)
		return addToGroupResponse{Error: err}
	} else if req.ActionType == "remove" {
		delete(s.state.Services[req.RemoveData.Group].Instances, req.RemoveData.InstanceID)

		if err := s.storage.SaveState(s.state); err != nil {
			return removeFromGroupResponse{Error: err}
		}
		return removeFromGroupResponse{}
	} else if req.ActionType == "get" {
		return getStateResponse{State: s.state, Error: nil}
	}

	return nil
}

func (s *Store) GetState() (*State, error) {
	return s.state, nil
}

// Snapshot is used to create a snapshot of the registry state
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.log.Info("creating snapshot")
	data, err := json.Marshal(s.state)
	if err != nil {
		return nil, err
	}

	return &snapshot{
		created: time.Now(),
		encoded: data,
	}, nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(s.encoded)
	if err != nil {
		return err
	}

	return nil
}

// Release is used to release the snapshot
func (s *snapshot) Release() {
}

// Restore is used to restore the snapshot
func (s *Store) Restore(rc io.ReadCloser) error {
	var state State
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}

	s.state = &state
	if err := s.storage.SaveState(s.state); err != nil {
		return err
	}

	return nil
}

// IsLeader returns true if the node is the leader
func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

// Close closes the Raft store
func (s *Store) Close() error {
	future := s.raft.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}

	return nil
}

// Join is used to join a node to the Raft cluster
func (s *Store) Join(id, addr string) error {
	s.log.Info("raft: joining node", zap.String("id", id), zap.String("addr", addr))
	if !s.IsLeader() {
		return ErrNotLeader
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	for _, server := range future.Configuration().Servers {
		if server.ID == serverID || server.Address == serverAddr {
			if server.ID == serverID && server.Address == serverAddr {
				s.log.Info("raft: node already joined", zap.String("id", id), zap.String("addr", addr))
				return nil
			}

			removeFuture := s.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := s.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		if err == raft.ErrNotLeader {
			s.log.Info("raft: not leader, retrying", zap.String("id", id), zap.String("addr", addr))
			return ErrNotLeader
		}
		return err
	}

	s.log.Info("raft: node joined", zap.String("id", id), zap.String("addr", addr))
	return nil
}

// Leave is used to leave the Raft cluster
func (s *Store) Leave(id string) error {
	s.log.Info("raft: leaving node", zap.String("id", id))
	if !s.IsLeader() {
		return ErrNotLeader
	}

	f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
	if err := f.Error(); err != nil {
		if err == raft.ErrNotLeader {
			s.log.Info("raft: not leader", zap.String("id", id))
			return ErrNotLeader
		}
		return err
	}

	s.log.Info("raft: node left", zap.String("id", id))
	return nil
}

func (s *Store) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			l := s.LeaderAddr()
			if l != "" {
				return nil
			}
		case <-timer.C:
			return errors.New("timed out waiting for leader")
		}
	}
}

func (s *Store) apply(req *ApplyRequest) (interface{}, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	future := s.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, err
	}

	res := future.Response()
	return res, nil
}

func (s *Store) AddToGroup(group string, instance Instance) error {
	res, err := s.apply(AddToGroup(group, instance))
	if err != nil {
		return err
	}

	if res != nil {
		return res.(addToGroupResponse).Error
	}
	return nil
}

func (s *Store) RemoveFromGroup(group string, instanceID string) error {
	res, err := s.apply(RemoveFromGroup(group, instanceID))
	if err != nil {
		return err
	}

	if res != nil {
		return res.(removeFromGroupResponse).Error
	}

	return nil
}

func (s *Store) LeaderAddr() string {
	leader, _ := s.raft.LeaderWithID()
	return string(leader)
}

// SetLoadBalancer sets the load balancer for a group
func (s *Store) SetLoadBalancer(group string, lbType LoadBalancingAlgorithm) error {
	s.lbMu.Lock()
	defer s.lbMu.Unlock()
	var lb LoadBalancer
	switch lbType {
	case LoadBalancingAlgorithmRoundRobin:
		lb = NewRoundRobinBalancer(s)
	default:
		return fmt.Errorf("unknown load balancing algorithm: %s", lbType)
	}

	s.lbs[group] = lb
	return nil
}

// GetBalancedNextInstance gets the next instance to use based on the load balancer
func (s *Store) GetBalancedNextInstance(group string) (*Instance, error) {
	s.lbMu.RLock()
	lb, ok := s.lbs[group]
	s.lbMu.RUnlock()

	if !ok {
		lb = NewRoundRobinBalancer(s)
		s.SetLoadBalancer(group, LoadBalancingAlgorithmRoundRobin)
	}

	return lb.Next(group)
}

// RecordMetrics records the metrics for an instance
func (s *Store) RecordMetrics(group string, instanceID string, connections int64) {
	s.lbMu.RLock()
	lb, ok := s.lbs[group]
	s.lbMu.RUnlock()

	if ok {
		lb.Record(instanceID, connections)
	}
}
