package gitlab

import (
	"fmt"

	"github.com/opentofu/opentofu/internal/backend"
	"github.com/opentofu/opentofu/internal/states/remote"
	"github.com/opentofu/opentofu/internal/states/statemgr"
)

// Workspaces returns a list of names for the workspaces found in Gitlab.
// The default workspace is always returned as the first element in the slice.
func (b *Backend) Workspaces() ([]string, error) {
	return b.client.List()
}

func (b *Backend) StateMgr(stateName string) (statemgr.Full, error) {
	return remote.NewState(b.remoteClientFor(stateName), b.encryption), nil
}

func (b *Backend) DeleteWorkspace(stateName string, _ bool) error {
	if stateName == backend.DefaultStateName || stateName == "" {
		return fmt.Errorf("can't delete default state")
	}

	return b.remoteClientFor(stateName).Delete()
}
