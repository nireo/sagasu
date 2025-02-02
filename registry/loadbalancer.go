package registry

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrNoInstances is returned when no instances are found
	ErrNoInstances = errors.New("no instances found")
)

type LoadBalancingAlgorithm string

const (
	LoadBalancingAlgorithmRoundRobin LoadBalancingAlgorithm = "round_robin"
	LoadBalancingAlgorithmRandom     LoadBalancingAlgorithm = "random"
	LoadBalancingAlgorithmLeastConn  LoadBalancingAlgorithm = "least_conn"
	LoadBalancingAlgorithmWeighted   LoadBalancingAlgorithm = "weighted"
)

// LoadBalancer is an interface for load balancing algorithms
type LoadBalancer interface {
	// Next returns the next instance to use based on the load balancing algorithm
	Next(group string) (*Instance, error)
	// Record records a new connection to an instance
	Record(instanceID string, connections int64)
}

// RoundRobinBalancer is a load balancer that implements the Round Robin algorithm
// meaning it will have a counter for each group and it will increment the counter
// and return the instance at the index of the counter
type RoundRobinBalancer struct {
	counters sync.Map
	store    *Store
}

// NewRoundRobinBalancer creates a new round robin balancer
func NewRoundRobinBalancer(store *Store) *RoundRobinBalancer {
	return &RoundRobinBalancer{
		store: store,
	}
}

// Next returns the next instance to use based on the load balancing algorithm
func (r *RoundRobinBalancer) Next(group string) (*Instance, error) {
	state, err := r.store.GetState()
	if err != nil {
		return nil, err
	}

	groupState, ok := state.Services[group]
	if !ok || len(groupState.Instances) == 0 {
		return nil, ErrNoInstances
	}

	counterVal, _ := r.counters.LoadOrStore(group, &atomic.Uint64{})
	counter := counterVal.(*atomic.Uint64)

	var healthyInstances []*Instance
	for _, instance := range groupState.Instances {
		if instance.Status == StatusHealthy {
			healthyInstances = append(healthyInstances, instance)
		}
	}

	if len(healthyInstances) == 0 {
		return nil, ErrNoInstances
	}

	idx := int(counter.Add(1)) % len(healthyInstances)
	return healthyInstances[idx], nil
}

// Record records a new connection to an instance
func (r *RoundRobinBalancer) Record(instanceID string, connections int64) {
	// Round robin balancer doesn't need to record connections
}
