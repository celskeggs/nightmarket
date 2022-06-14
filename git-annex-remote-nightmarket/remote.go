package main

import (
	"fmt"
	"github.com/celskeggs/nightmarket/lib/annexremote"
	"github.com/celskeggs/nightmarket/lib/cryptapi"
	"github.com/hashicorp/go-multierror"
	"io"
	"os"
	"time"
)

const ResyncDelay = 10 * time.Second

type Helper struct {
	Clerk *cryptapi.Clerk
	// note: objectlist may become stale, so recheck if it really matters!
	ObjectList []string
	LastUpdate time.Time
}

func (h *Helper) ListConfigs() ([]annexremote.Config, error) {
	return []annexremote.Config{
		{
			Name:        "configfile",
			Description: "path to nightmarket configuration file",
		},
	}, nil
}

func (h *Helper) loadConfigFile(a *annexremote.GitAnnex) (*cryptapi.Clerk, error) {
	configPath, err := a.GetConfig("configfile")
	if err != nil {
		return nil, err
	}
	if configPath == "" {
		return nil, fmt.Errorf("no 'configfile' setting configured")
	}
	return cryptapi.LoadConfig(configPath)
}

func (h *Helper) InitRemote(a *annexremote.GitAnnex) error {
	_, err := h.loadConfigFile(a)
	return err
}

func (h *Helper) invalidateCache() {
	h.LastUpdate = time.Time{}
}

func (h *Helper) syncList() error {
	if !h.LastUpdate.IsZero() && time.Now().Before(h.LastUpdate.Add(ResyncDelay)) {
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

func (h *Helper) Prepare(a *annexremote.GitAnnex) error {
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
func (h *Helper) keyToInfix(key string) string {
	return "upload-" + h.Clerk.HMAC(key)
}

func (h *Helper) findByKey(key string) (path string, err error) {
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

func (h *Helper) locateFile(key string) (path string, err error) {
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

func (h *Helper) TransferRetrieve(a *annexremote.GitAnnex, key string, tempfilepath string) (err error) {
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

func (h *Helper) CheckPresent(a *annexremote.GitAnnex, key string) (present bool, err error) {
	path, err := h.locateFile(key)
	if err != nil {
		return false, err
	}
	return path != "", nil
}

func (h *Helper) TransferStore(a *annexremote.GitAnnex, key string, tempfilepath string) (err error) {
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
	// validate it got uploaded correctly
	h.invalidateCache()
	path, err = h.locateFile(key)
	if err != nil {
		return err
	}
	if path != newPath {
		return fmt.Errorf(
			"transfer store command uploaded key %q as path %q, but locateFile produced path %q", key, newPath, path)
	}
	return nil
}

func (h *Helper) Remove(a *annexremote.GitAnnex, key string) error {
	return fmt.Errorf("files cannot be removed from the nightmarket remote (by design)")
}

func main() {
	annexremote.Mainloop(&Helper{})
}
