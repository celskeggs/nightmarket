package annexhelper

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/celskeggs/nightmarket/lib/annexremote"
	"github.com/celskeggs/nightmarket/lib/cryptapi"
	"github.com/hashicorp/go-multierror"
)

const resyncDelay = 10 * time.Second

type helper struct {
	Clerk *cryptapi.Clerk
	// note: objectlist may become stale, so recheck if it really matters!
	ObjectList []string
	LastUpdate time.Time
}

func (h *helper) NegotiateAsync() bool {
	return false
}

func (h *helper) ListConfigs() ([]annexremote.Config, error) {
	return []annexremote.Config{
		{
			Name:        "underlying",
			Description: "git remote to retrieve underlying configuration for",
		},
	}, nil
}

func (h *helper) loadConfigFile(a *annexremote.Responder) (*cryptapi.Clerk, error) {
	underlying, err := a.GetConfig("underlying")
	if err != nil {
		return nil, err
	}
	if underlying == "" {
		return nil, fmt.Errorf("no 'configfile' setting configured")
	}
	gitDir, err := a.GetGitDir()
	if err != nil {
		return nil, err
	}
	if gitDir == "" {
		return nil, fmt.Errorf("invalid empty GIT_DIR setting detected")
	}
	cmd := exec.Command("git", "remote", "get-url", "--", underlying)
	cmd.Env = append(os.Environ(), "GIT_DIR="+gitDir)
	configURLBytes, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("while trying to run %q: %w", cmd, err)
	}
	configURL := strings.TrimSpace(string(configURLBytes))
	const NightmarketPrefix = "nightmarket::"
	if !strings.HasPrefix(configURL, NightmarketPrefix) {
		return nil, fmt.Errorf("invalid URL for nightmarket remote %q: %q", underlying, configURL)
	}
	return cryptapi.LoadConfig(configURL[len(NightmarketPrefix):])
}

func (h *helper) InitRemote(a *annexremote.Responder) error {
	_, err := h.loadConfigFile(a)
	return err
}

func (h *helper) syncList() error {
	if !h.LastUpdate.IsZero() && time.Now().Before(h.LastUpdate.Add(resyncDelay)) {
		// continue using previous ObjectList data
		return nil
	}
	if h.Clerk == nil {
		return fmt.Errorf("clerk not initialized; maybe we didn't get a PREPARE yet")
	}
	objects, err := h.Clerk.ListObjects()
	if err != nil {
		return err
	}
	h.ObjectList = objects
	h.LastUpdate = time.Now()
	return nil
}

func (h *helper) Prepare(a *annexremote.Responder) error {
	clerk, err := h.loadConfigFile(a)
	if err != nil {
		return err
	}
	h.Clerk = clerk
	if err := h.syncList(); err != nil {
		return err
	}
	return nil
}

// reproducible filename hash
func (h *helper) keyToInfix(key string) string {
	return "upload-" + h.Clerk.HMAC(key)
}

func (h *helper) findByKey(key string) (path string, err error) {
	if h.Clerk == nil {
		return "", fmt.Errorf("clerk not initialized; maybe we didn't get a PREPARE yet")
	}
	cryptedFilename := h.keyToInfix(key)
	path = "" // return value if not found
	for _, objectPath := range h.ObjectList {
		_, infix, _, err := cryptapi.SplitPath(objectPath)
		if err != nil {
			return "", err
		}
		if infix == cryptedFilename {
			if path != "" {
				return "", fmt.Errorf("detected duplicate files for key %q: %q and %q", key, path, objectPath)
			}
			path = objectPath
		}
	}
	return path, nil
}

func (h *helper) locateFile(key string) (path string, err error) {
	// first do a check to see if we've already located the file, without any network traffic
	path, err = h.findByKey(key)
	if err != nil {
		return "", err
	}
	if path != "" {
		return path, nil
	}
	// did not find it, so sync against the remote (if it's been more than the timeout)
	if err := h.syncList(); err != nil {
		return "", err
	}
	// check again, and whatever this result is, it's final.
	return h.findByKey(key)
}

func (h *helper) TransferRetrieve(a *annexremote.Responder, key string, tempfilepath string) (err error) {
	// TODO: report progress messages
	path, err := h.locateFile(key)
	if err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("no such key detected in repository during transfer retrieve: %q", key)
	}
	wf, err := os.Create(tempfilepath)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := wf.Close(); err2 != nil {
			err = multierror.Append(err, err2)
		}
	}()
	rc, err := h.Clerk.GetDecryptObjectStream(path)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := rc.Close(); err2 != nil {
			err = multierror.Append(err, err2)
		}
	}()
	if _, err = io.Copy(wf, rc); err != nil {
		return err
	}
	return nil
}

func (h *helper) CheckPresent(a *annexremote.Responder, key string) (present bool, err error) {
	path, err := h.locateFile(key)
	if err != nil {
		return false, err
	}
	return path != "", nil
}

func (h *helper) TransferStore(a *annexremote.Responder, key string, tempfilepath string) (err error) {
	path, err := h.locateFile(key)
	if err != nil {
		return err
	}
	if path != "" {
		return fmt.Errorf("attempt to upload file %q that already exists on the remote", key)
	}
	f, err := os.Open(tempfilepath)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := f.Close(); err2 != nil {
			err = multierror.Append(err, err2)
		}
	}()
	newPath, err := h.Clerk.PutEncryptObjectStream(h.keyToInfix(key), f)
	if err != nil {
		return err
	}
	// add the new path to the cached list, to avoid an unnecessary round trip
	h.ObjectList = append(h.ObjectList, newPath)
	return nil
}

func (h *helper) Remove(a *annexremote.Responder, key string) error {
	return fmt.Errorf("files cannot be removed from the nightmarket remote (by design)")
}

func Init() annexremote.Helper {
	return &helper{}
}
