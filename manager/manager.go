package manager

import "github.com/nireo/sagasu/registry"

type Manager struct {
	store   *registry.Store
	hc      *registry.HealthChecker
	closeCh chan struct{}
}

// NewManager creates a new manager
func NewManager(store *registry.Store, hc *registry.HealthChecker) *Manager {
	return &Manager{store: store, hc: hc, closeCh: make(chan struct{})}
}

// Start starts the manager
func (m *Manager) Start() error {
	m.hc.Start()

	<-m.closeCh
	m.hc.Stop()
	m.store.Close()

	return nil
}

// Stop stops the manager
func (m *Manager) Stop() error {
	close(m.closeCh)

	return nil
}
