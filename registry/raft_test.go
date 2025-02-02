package registry

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func newTestStore(t *testing.T, id int, bootstrap bool) (*Store, error) {
	datadir, err := os.MkdirTemp("", fmt.Sprintf("sagasu-raft-test-%d", id))
	if err != nil {
		return nil, err
	}

	port, err := getFreePort()
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
	conf.Raft.SnapshotInterval = 3 * time.Second
	conf.Raft.DataDir = datadir

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
		store, err := newTestStore(t, i, i == 0)
		assert.NoError(t, err)
		stores[i] = store

		if i != 0 {
			err = stores[0].Join(fmt.Sprintf("%d", i), stores[i].config.Transport.Addr().String())
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		} else {
			err = stores[0].WaitForLeader(3 * time.Second)
			assert.NoError(t, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	stores[0].AddToGroup("test", Instance{ID: "test"})
	time.Sleep(500 * time.Millisecond)

	state, err := stores[1].GetState()
	assert.NoError(t, err)
	assert.Equal(t, state.Services["test"].Instances["test"].ID, "test")
}

type testSnapshotSink struct {
	file *os.File
}

func newTestSnapshotSink(path string) (*testSnapshotSink, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &testSnapshotSink{file: f}, nil
}

func (s *testSnapshotSink) Write(p []byte) (n int, err error) {
	return s.file.Write(p)
}

func (s *testSnapshotSink) Close() error {
	return s.file.Close()
}

func (s *testSnapshotSink) ID() string {
	return "test-snapshot"
}

func (s *testSnapshotSink) Cancel() error {
	if err := s.file.Close(); err != nil {
		return err
	}
	return os.Remove(s.file.Name())
}

func TestSnapshotIntegration(t *testing.T) {
	nodeCount := 3
	stores := make([]*Store, nodeCount)

	for i := 0; i < nodeCount; i++ {
		store, err := newTestStore(t, i, i == 0)
		assert.NoError(t, err)
		stores[i] = store

		if i != 0 {
			err = stores[0].Join(fmt.Sprintf("%d", i), stores[i].config.Transport.Addr().String())
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		} else {
			err = stores[0].WaitForLeader(3 * time.Second)
			assert.NoError(t, err)
		}
	}

	time.Sleep(1 * time.Second)

	err := stores[0].AddToGroup("test-group", Instance{ID: "test1"})
	require.NoError(t, err)
	err = stores[0].AddToGroup("test-group", Instance{ID: "test2"})
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	for i, store := range stores {
		snap, err := store.Snapshot()
		require.NoError(t, err)

		tmpDir := t.TempDir()
		sink, err := newTestSnapshotSink(filepath.Join(tmpDir, "node-snapshot"))
		require.NoError(t, err)

		err = snap.Persist(sink)
		require.NoError(t, err)

		snapshotData, err := os.ReadFile(filepath.Join(tmpDir, "node-snapshot"))
		require.NoError(t, err)

		var state State
		err = json.Unmarshal(snapshotData, &state)
		require.NoError(t, err)

		assert.Contains(t, state.Services, "test-group", "Node %d missing test-group", i)
		assert.Contains(t, state.Services["test-group"].Instances, "test1", "Node %d missing test1", i)
		assert.Contains(t, state.Services["test-group"].Instances, "test2", "Node %d missing test2", i)
	}
}

type storeWithPath struct {
	store *Store
	path  string
}

type TestCluster struct {
	stores []storeWithPath
}

func (ts *TestCluster) Close() {
	for _, store := range ts.stores {
		store.store.Close()
		os.RemoveAll(store.path)
	}
}

func createTestCluster(t *testing.T, nodeCount int) (*TestCluster, error) {
	stores := make([]storeWithPath, nodeCount)

	for i := 0; i < nodeCount; i++ {
		store, err := newTestStore(t, i, i == 0)
		if err != nil {
			return nil, err
		}
		stores[i] = storeWithPath{store: store, path: store.config.Raft.DataDir}

		if i != 0 {
			err = stores[0].store.Join(fmt.Sprintf("%d", i), stores[i].store.config.Transport.Addr().String())
			if err != nil {
				return nil, err
			}
			time.Sleep(100 * time.Millisecond)
		} else {
			err = stores[0].store.WaitForLeader(3 * time.Second)
			if err != nil {
				return nil, err
			}
		}
	}

	return &TestCluster{stores: stores}, nil
}

func setupTestServer(t *testing.T, healty bool) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healty {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	return server
}

func TestHealthChecker(t *testing.T) {
	healthyServer := setupTestServer(t, true)
	unhealthyServer := setupTestServer(t, false)
	defer healthyServer.Close()
	defer unhealthyServer.Close()
	var err error

	nodeCount := 3
	stores := make([]*Store, nodeCount)

	for i := 0; i < nodeCount; i++ {
		store, err := newTestStore(t, i, i == 0)
		assert.NoError(t, err)
		stores[i] = store

		if i != 0 {
			err = stores[0].Join(fmt.Sprintf("%d", i), stores[i].config.Transport.Addr().String())
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		} else {
			err = stores[0].WaitForLeader(3 * time.Second)
			assert.NoError(t, err)
		}
	}

	// Wait for leader election and cluster stability
	err = stores[0].WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	err = stores[0].AddToGroup("test-group", Instance{
		ID:      "healthy-instance",
		Address: healthyServer.URL,
	})
	require.NoError(t, err)

	err = stores[0].AddToGroup("test-group", Instance{
		ID:      "unhealthy-instance",
		Address: unhealthyServer.URL,
	})
	require.NoError(t, err)

	// Wait for state replication
	time.Sleep(1 * time.Second)

	logger, _ := zap.NewDevelopment()
	hc := NewHealthChecker(stores[0], 100*time.Millisecond, 50*time.Millisecond, logger)
	hc.Start()
	defer hc.Stop()

	// Use polling instead of fixed sleep
	require.Eventually(t, func() bool {
		status, exists := hc.GetStatus("test-group", "healthy-instance")
		return exists && status == StatusHealthy
	}, 3*time.Second, 100*time.Millisecond, "Healthy instance status not updated")

	require.Eventually(t, func() bool {
		status, exists := hc.GetStatus("test-group", "unhealthy-instance")
		return exists && status == StatusUnhealthy
	}, 3*time.Second, 100*time.Millisecond, "Unhealthy instance status not updated")

	// Wait for removal of unhealthy instance
	require.Eventually(t, func() bool {
		state, err := stores[0].GetState()
		if err != nil {
			return false
		}
		_, exists := state.Services["test-group"].Instances["unhealthy-instance"]
		return !exists
	}, 3*time.Second, 100*time.Millisecond, "Unhealthy instance not removed")
}
