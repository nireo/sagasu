package registry

import (
	"context"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

type HealthChecker struct {
	store    *Store
	interval time.Duration
	timeout  time.Duration
	log      *zap.Logger
	client   *http.Client
	stopCh   chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex
	results  map[string]map[string]ServiceStatus
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(store *Store, interval time.Duration, timeout time.Duration, log *zap.Logger) *HealthChecker {
	return &HealthChecker{
		store:    store,
		interval: interval,
		timeout:  timeout,
		log:      log,
		client:   http.DefaultClient,
		stopCh:   make(chan struct{}),
		results:  make(map[string]map[string]ServiceStatus),
	}
}

// Start starts the health checker
func (hc *HealthChecker) Start() {
	hc.wg.Add(1)
	go hc.run()
}

// Stop stops the health checker
func (hc *HealthChecker) Stop() {
	close(hc.stopCh)
	hc.wg.Wait()
}

// run is the main loop for the health checker
func (hc *HealthChecker) run() {
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.stopCh:
			return
		case <-ticker.C:
			hc.checkAll()
		}
	}
}

func (hc *HealthChecker) checkAll() {
	state, err := hc.store.GetState()
	if err != nil {
		hc.log.Error("failed to get state", zap.Error(err))
		return
	}

	var wg sync.WaitGroup
	for groupName, group := range state.Services {
		for instanceID, instance := range group.Instances {
			wg.Add(1)
			go func(groupName string, instanceID string, instance *Instance) {
				defer wg.Done()
				status := hc.checkInstance(groupName, instanceID, instance)
				hc.updateStatus(groupName, instanceID, status)
				if status == StatusUnhealthy {
					hc.store.RemoveFromGroup(groupName, instanceID)
				}
			}(groupName, instanceID, instance)
		}
	}
	wg.Wait()
}

func (hc *HealthChecker) checkInstance(groupName string, instanceID string, instance *Instance) ServiceStatus {
	hc.log.Info("checking instance", zap.String("group", groupName), zap.String("instance", instanceID))
	ctx, cancel := context.WithTimeout(context.Background(), hc.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", instance.Address+"/health", nil)
	if err != nil {
		return StatusUnhealthy
	}

	resp, err := hc.client.Do(req)
	if err != nil {
		return StatusUnhealthy
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return StatusUnhealthy
	}

	return StatusHealthy
}

func (hc *HealthChecker) updateStatus(groupName string, instanceID string, status ServiceStatus) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if _, exists := hc.results[groupName]; !exists {
		hc.results[groupName] = make(map[string]ServiceStatus)
	}
	hc.results[groupName][instanceID] = status
}

func (hc *HealthChecker) GetStatus(group string, instanceID string) (ServiceStatus, bool) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	status, ok := hc.results[group][instanceID]
	return status, ok
}

func (hc *HealthChecker) GetGroupStatus(group string) map[string]ServiceStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.results[group]
}
