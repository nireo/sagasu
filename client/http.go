package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/nireo/sagasu/registry"
)

type Client struct {
	addr string
}

func NewClient(addr string) *Client {
	return &Client{addr: addr}
}

func (c *Client) AddToGroup(group string, instance registry.AddToGroupRequest) error {
	data, err := json.Marshal(instance)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/state", c.addr), bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to add to group: %s", resp.Status)
	}

	return nil
}

func (c *Client) RemoveFromGroup(group string, instanceID string) error {
	data, err := json.Marshal(registry.RemoveFromGroupRequest{
		Group:      group,
		InstanceID: instanceID,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/state", c.addr), bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to remove from group: %s", resp.Status)
	}

	return nil
}
