package registry

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	return l.Addr().(*net.TCPAddr).Port, nil
}

func newTestStore(t *testing.T, port, id int, bootstrap bool) (*Store, error) {
	datadir, err := os.MkdirTemp("", "sagasu-raft-test")
	if err != nil {
		return nil, err
	}

	conf := Config{}
	conf.Raft.Bootstrap = bootstrap
	conf.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", id))
	conf.Raft.HeartbeatTimeout = 50 * time.Millisecond
	conf.Raft.ElectionTimeout = 50 * time.Millisecond
	conf.Raft.Bootstrap = bootstrap
	conf.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.Raft.CommitTimeout = 5 * time.Millisecond
	conf.Raft.SnapshotThreshold = 10000

	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}

	conf.Transport = &Transport{
		ln: ln,
	}

	store, err := NewStore(&conf)
	if err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		store.Close()
		os.RemoveAll(datadir)
	})

	return store, nil
}

func TestRaft(t *testing.T) {
	nodeCount := 3
	stores := make([]*Store, nodeCount)

	for i := 0; i < nodeCount; i++ {
		port, err := getFreePort()
		assert.NoError(t, err)

		store, err := newTestStore(t, port, i, i == 0)
		assert.NoError(t, err)
		stores[i] = store

		if i != 0 {
			err = stores[0].Join(fmt.Sprintf("%d", i), stores[i].config.Transport.Addr().String())
			assert.NoError(t, err)
		} else {
			err = stores[0].WaitForLeader(3 * time.Second)
			assert.NoError(t, err)
		}
	}

	stores[0].AddToGroup("test", Instance{ID: "test"})
	time.Sleep(500 * time.Millisecond)

	state, err := stores[1].GetState()
	assert.NoError(t, err)
	assert.Equal(t, state.Services["test"].Instances["test"].ID, "test")
}
