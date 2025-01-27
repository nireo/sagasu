package registry

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
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
	}
	Transport *Transport // Transport is the transport layer for Raft
}

// Store is the Raft store for the registry
type Store struct {
	config  *Config
	raftDir string
	raft    *raft.Raft
	log     *zap.Logger
	tn      *raft.NetworkTransport
}

// snapshot is a snapshot of the registry state.
type snapshot struct {
	created time.Time // the time the snapshot was created
	encoded []byte    // the registry state encoded as json
}

func (s *Store) setupRaft(dataDir string) error {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}

	s.raftDir = dataDir
	stablepb, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "raft.db"))
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), 3, os.Stdout)
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

func (s *Store) Apply(log *raft.Log) interface{} {
	return nil
}
