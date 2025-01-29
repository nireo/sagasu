package registry

// AddToGroupRequest is used to add an instance to a group
type AddToGroupRequest struct {
	Group    string   `json:"group"`
	Instance Instance `json:"instance"`
}

// RemoveFromGroupRequest is used to remove an instance from a group
type RemoveFromGroupRequest struct {
	Group      string `json:"group"`
	InstanceID string `json:"instance_id"`
}

// ApplyRequest is used to apply an action to the registry
type ApplyRequest struct {
	ActionType string                  `json:"action_type"`
	RemoveData *RemoveFromGroupRequest `json:"remove_data,omitempty"`
	AddData    *AddToGroupRequest      `json:"add_data,omitempty"`
}

// AddToGroup creates a new AddToGroupRequest
func AddToGroup(group string, instance Instance) *ApplyRequest {
	return &ApplyRequest{
		ActionType: "add",
		AddData: &AddToGroupRequest{
			Group:    group,
			Instance: instance,
		},
	}
}

// RemoveFromGroup creates a new RemoveFromGroupRequest
func RemoveFromGroup(group string, instanceID string) *ApplyRequest {
	return &ApplyRequest{
		ActionType: "remove",
		RemoveData: &RemoveFromGroupRequest{
			Group:      group,
			InstanceID: instanceID,
		},
	}
}
