package registry

import (
	"os"
	"reflect"
	"testing"
)

func TestRegistrySimple(t *testing.T) {
	if err := os.MkdirAll("./test", 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	storage, err := NewStorage("./test")
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()
	defer os.RemoveAll("./test")

	state := &State{
		Version: 10,
		Services: map[string]*Group{
			"test": {
				Instances: map[string]*Instance{
					"test": {
						ID: "test",
					},
				},
			},
		},
	}

	err = storage.SaveState(state)
	if err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	newState, err := storage.GetState()
	if err != nil {
		t.Fatalf("Failed to get state: %v", err)
	}

	if !reflect.DeepEqual(state, newState) {
		t.Fatalf("State mismatch: %v != %v", state, newState)
	}
}
