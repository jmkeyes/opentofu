package gitlab

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"

	"github.com/shurcooL/graphql"

	"github.com/opentofu/opentofu/internal/backend"
	"github.com/opentofu/opentofu/internal/states/remote"
	"github.com/opentofu/opentofu/internal/states/statemgr"
)

// FIXME: validate we follow the interface.
var _ remote.ClientLocker = &RemoteClient{}

type RemoteClient struct {
	BaseURL       *url.URL
	Project       string
	StateName     string
	HTTPClient    *http.Client
	GraphQLClient *graphql.Client

	lockID       string
	jsonLockInfo []byte
}

func (client *RemoteClient) List() ([]string, error) {
	var query struct {
		Project struct {
			TerraformStates struct {
				Nodes []struct {
					Name string
				}
			}
		} `graphql:"project(fullPath: $projectPath)"`
	}

	variables := map[string]interface{}{
		"projectPath": client.Project,
	}

	if err := client.GraphQLClient.Query(context.Background(), &query, variables); err != nil {
		return nil, fmt.Errorf("unable to execute GraphQL query: %w", err)
	}

	states := []string{backend.DefaultStateName}

	for _, v := range query.Project.TerraformStates.Nodes {
		if v.Name != backend.DefaultStateName {
			states = append(states, v.Name)
		}
	}

	// The default state always comes first.
	sort.Strings(states[1:])

	return states, nil
}

func (client *RemoteClient) stateURL() *url.URL {
	projectName := url.PathEscape(client.Project)
	stateName := url.PathEscape(client.StateName)

	// /api/v4/projects/{projectName}/terraform/state/{stateName}
	pathComponents := []string{
		"api", "v4",
		"projects", projectName,
		"terraform", "state", stateName,
	}

	return client.BaseURL.JoinPath(pathComponents...)
}

func (client *RemoteClient) Get() (*remote.Payload, error) {
	stateURL := client.stateURL().String()

	req, err := http.NewRequest(http.MethodGet, stateURL, nil)

	if err != nil {
		return nil, fmt.Errorf("create request failed: %w", err)
	}

	resp, err := client.HTTPClient.Do(req)

	if err != nil {
		return nil, fmt.Errorf("get state failed: %w", err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)

	if _, err := io.Copy(buf, resp.Body); err != nil {
		return nil, fmt.Errorf("failed to read remote state: %w", err)
	}

	if buf.Len() == 0 {
		return nil, nil
	}

	var (
		data   []byte = buf.Bytes()
		digest []byte
	)

	if raw := resp.Header.Get("Content-MD5"); raw != "" {
		hash, err := base64.StdEncoding.DecodeString(raw)

		if err != nil {
			return nil, fmt.Errorf("failed to decode Content-MD5 '%s': %w", raw, err)
		}

		digest = hash
	} else {
		hash := md5.Sum(data)
		digest = hash[:]
	}

	payload := &remote.Payload{
		Data: data,
		MD5:  digest,
	}

	return payload, nil
}

func (client *RemoteClient) Put(data []byte) error {
	stateURL := client.stateURL()

	if client.lockID != "" {
		query := stateURL.Query()
		query.Set("ID", client.lockID)
		stateURL.RawQuery = query.Encode()
	}

	req, err := http.NewRequest(http.MethodPost, stateURL.String(), bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")

	if len(data) > 0 {
		// Generate the MD5
		hash := md5.Sum(data)
		b64 := base64.StdEncoding.EncodeToString(hash[:])
		req.Header.Set("Content-MD5", b64)
	}

	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	resp, err := client.HTTPClient.Do(req)

	if err != nil {
		return fmt.Errorf("put state failed: %w", err)
	}

	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
		return nil
	default:
		return fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}
}

func (client *RemoteClient) Delete() error {
	stateUrl := client.stateURL().String()

	req, err := http.NewRequest(http.MethodDelete, stateUrl, nil)

	if err != nil {
		return err
	}

	resp, err := client.HTTPClient.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	default:
		return fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}
}

// POST {apiUrl}/projects/%s/terraform/state/%s/lock
func (client *RemoteClient) Lock(info *statemgr.LockInfo) (string, error) {
	lockURL := client.stateURL().JoinPath("lock").String()

	client.lockID = ""

	jsonLockInfo := info.Marshal()

	req, err := http.NewRequest(http.MethodPost, lockURL, bytes.NewReader(jsonLockInfo))
	req.Header.Set("Content-Type", "application/json")

	if len(jsonLockInfo) > 0 {
		// Generate the MD5
		hash := md5.Sum(jsonLockInfo)
		b64 := base64.StdEncoding.EncodeToString(hash[:])
		req.Header.Set("Content-MD5", b64)
	}

	if err != nil {
		return "", err
	}

	resp, err := client.HTTPClient.Do(req)

	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		client.lockID = info.ID
		client.jsonLockInfo = jsonLockInfo
		return info.ID, nil
	case http.StatusUnauthorized:
		return "", fmt.Errorf("HTTP remote state endpoint requires auth")
	case http.StatusForbidden:
		return "", fmt.Errorf("HTTP remote state endpoint invalid auth")
	case http.StatusConflict, http.StatusLocked:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", &statemgr.LockError{
				Info: info,
				Err:  fmt.Errorf("HTTP remote state already locked, failed to read body"),
			}
		}
		existing := statemgr.LockInfo{}
		err = json.Unmarshal(body, &existing)
		if err != nil {
			return "", &statemgr.LockError{
				Info: info,
				Err:  fmt.Errorf("HTTP remote state already locked, failed to unmarshal body"),
			}
		}
		return "", &statemgr.LockError{
			Info: info,
			Err:  fmt.Errorf("HTTP remote state already locked: ID=%s", existing.ID),
		}
	default:
		return "", fmt.Errorf("unexpected HTTP response code %d", resp.StatusCode)
	}
}

// DELETE {apiUrl}/projects/%s/terraform/state/%s/lock
func (client *RemoteClient) Unlock(id string) error {
	unlockURL := client.stateURL().JoinPath("lock").String()

	req, err := http.NewRequest(http.MethodDelete, unlockURL, bytes.NewReader(client.jsonLockInfo))
	req.Header.Set("Content-Type", "application/json")

	if len(client.jsonLockInfo) > 0 {
		// Generate the MD5
		hash := md5.Sum(client.jsonLockInfo)
		b64 := base64.StdEncoding.EncodeToString(hash[:])
		req.Header.Set("Content-MD5", b64)
	}

	if err != nil {
		return err
	}

	resp, err := client.HTTPClient.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	default:
		return fmt.Errorf("unexpected HTTP response code %d", resp.StatusCode)
	}
}
