package registry

import (
	"encoding/json"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ServiceStatus represents the health state of a service instance
type ServiceStatus string

const (
	// StatusHealthy indicates the service instance is operating normally
	StatusHealthy ServiceStatus = "healthy"
	// StatusUnhealthy indicates the service instance is not operating properly
	StatusUnhealthy ServiceStatus = "unhealthy"
	// StatusDraining indicates the service instance is preparing to shut down
	StatusDraining ServiceStatus = "draining"
)

// State represents the complete registry state including all services
type State struct {
	Services map[string]*Group `json:"services"`
	Version  uint64            `json:"version"`
}

// Group represents a collection of service instances with shared configuration
type Group struct {
	Instances map[string]*Instance `json:"instances"`
	Config    GroupConfig          `json:"config"`
}

// Instance represents a single running instance of a service
type Instance struct {
	ID           string            `json:"id"`
	Address      string            `json:"address"`
	Port         int               `json:"port"`
	Status       ServiceStatus     `json:"status"`
	LastHearbeat time.Time         `json:"last_heartbeat"`
	Metadata     map[string]string `json:"metadata"`
}

// GroupConfig defines configuration parameters for a service group
type GroupConfig struct {
	TTL          time.Duration `json:"ttl"`
	MaxInstances int           `json:"max_instances"`
	MinInstances int           `json:"min_instances"`
}

// PersistantStorage handles persistent storage of registry state using BoltDB
type PersistantStorage struct {
	db *bolt.DB
}

// NewStorage creates a new persistent storage instance at the specified path
func NewStorage(path string) (*PersistantStorage, error) {
	db, err := bolt.Open(filepath.Join(path, "sagasu_store.db"), 0600, nil)
	if err != nil {
		return nil, err
	}

	return &PersistantStorage{db: db}, nil
}

// Close cleanly shuts down the storage database
func (s *PersistantStorage) Close() error {
	return s.db.Close()
}

// SaveState persists the current registry state to storage
func (s *PersistantStorage) SaveState(state *State) error {
	// TODO: make this more efficient since this is pretty stupid
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("registry"))
		if err != nil {
			return err
		}

		data, err := json.Marshal(state)
		if err != nil {
			return err
		}

		bucket.Put([]byte("state"), data)
		return nil
	})
}

// GetState retrieves the current registry state from storage
func (s *PersistantStorage) GetState() (*State, error) {
	var state State

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("registry"))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte("state"))
		return json.Unmarshal(data, &state)
	})

	return &state, err
}
