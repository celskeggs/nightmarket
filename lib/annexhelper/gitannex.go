package annexhelper

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/celskeggs/nightmarket/lib/annexremote"
	"github.com/celskeggs/nightmarket/lib/cryptapi"
	"github.com/hashicorp/go-multierror"
)

type void struct{}

const resyncDelay = 10 * time.Second

type helper struct {
	ClerkLock  sync.Mutex
	ClerkMaybe *cryptapi.Clerk

	// note: objectlist may become stale, so recheck if it really matters!
	ObjectLock       sync.Mutex
	ObjectListLocked []string
	LastUpdateLocked time.Time

	KeyLocksLock sync.Mutex
	KeyLocksCond sync.Cond
	KeyLocks     map[string]void
}

func (h *helper) NegotiateAsync() bool {
	return true
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

func (h *helper) prepareClerk(a *annexremote.Responder) error {
	h.ClerkLock.Lock()
	defer h.ClerkLock.Unlock()
	if h.ClerkMaybe == nil {
		clerk, err := h.loadConfigFile(a)
		if err != nil {
			return err
		}
		h.ClerkMaybe = clerk
	}
	return nil
}

func (h *helper) getClerk() (*cryptapi.Clerk, error) {
	h.ClerkLock.Lock()
	defer h.ClerkLock.Unlock()
	if h.ClerkMaybe == nil {
		return nil, fmt.Errorf("clerk not initialized; maybe we didn't get a PREPARE yet")
	}
	return h.ClerkMaybe, nil
}

func (h *helper) syncList() ([]string, error) {
	h.ObjectLock.Lock()
	defer h.ObjectLock.Unlock()
	if !h.LastUpdateLocked.IsZero() && time.Now().Before(h.LastUpdateLocked.Add(resyncDelay)) {
		// continue using previous ObjectList data
		return nil, nil
	}
	clerk, err := h.getClerk()
	if err != nil {
		return nil, err
	}
	objects, err := clerk.ListObjects()
	if err != nil {
		return nil, err
	}
	h.ObjectListLocked = objects
	h.LastUpdateLocked = time.Now()
	return objects, nil
}

func (h *helper) addObjectToList(object string) {
	h.ObjectLock.Lock()
	defer h.ObjectLock.Unlock()
	h.ObjectListLocked = append(h.ObjectListLocked, object)
}

func (h *helper) getObjectList() ([]string, error) {
	h.ObjectLock.Lock()
	defer h.ObjectLock.Unlock()
	if h.LastUpdateLocked.IsZero() {
		return nil, errors.New("object list not populated")
	}
	return h.ObjectListLocked, nil
}

func (h *helper) Prepare(a *annexremote.Responder) error {
	if err := h.prepareClerk(a); err != nil {
		return err
	}
	if _, err := h.syncList(); err != nil {
		return err
	}
	return nil
}

func (h *helper) lockKey(key string) {
	h.KeyLocksLock.Lock()
	defer h.KeyLocksLock.Unlock()
	for {
		_, found := h.KeyLocks[key]
		if !found {
			h.KeyLocks[key] = void{}
			return
		}
		h.KeyLocksCond.Wait()
	}
}

func (h *helper) unlockKey(key string) {
	h.KeyLocksLock.Lock()
	defer h.KeyLocksLock.Unlock()
	_, found := h.KeyLocks[key]
	if !found {
		panic("attempt to unlock key that was not locked")
	}
	delete(h.KeyLocks, key)
	h.KeyLocksCond.Broadcast()
}

// reproducible filename hash
func keyToInfix(clerk *cryptapi.Clerk, key string) string {
	return "upload-" + clerk.HMAC(key)
}

func (h *helper) findByKey(key string, objects []string) (path string, err error) {
	clerk, err := h.getClerk()
	if err != nil {
		return "", err
	}
	cryptedFilename := keyToInfix(clerk, key)
	path = "" // return value if not found
	for _, objectPath := range objects {
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
	objects, err := h.getObjectList()
	if err != nil {
		return "", err
	}
	path, err = h.findByKey(key, objects)
	if err != nil {
		return "", err
	}
	if path != "" {
		return path, nil
	}
	// did not find it, so sync against the remote (if it's been more than the timeout)
	objects, err = h.syncList()
	if err != nil {
		return "", err
	}
	// check again, and whatever this result is, it's final.
	return h.findByKey(key, objects)
}

func (h *helper) TransferRetrieve(a *annexremote.Responder, key string, tempfilepath string) (err error) {
	h.lockKey(key)
	defer h.unlockKey(key)

	// TODO: report progress messages
	clerk, err := h.getClerk()
	if err != nil {
		return err
	}
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
	rc, err := clerk.GetDecryptObjectStream(path)
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
	h.lockKey(key)
	defer h.unlockKey(key)

	path, err := h.locateFile(key)
	if err != nil {
		return false, err
	}
	return path != "", nil
}

func (h *helper) TransferStore(a *annexremote.Responder, key string, tempfilepath string) (err error) {
	h.lockKey(key)
	defer h.unlockKey(key)

	clerk, err := h.getClerk()
	if err != nil {
		return err
	}
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
	newPath, err := clerk.PutEncryptObjectStream(keyToInfix(clerk, key), f)
	if err != nil {
		return err
	}
	// add the new path to the cached list, to avoid an unnecessary round trip
	h.addObjectToList(newPath)
	return nil
}

func (h *helper) Remove(a *annexremote.Responder, key string) error {
	return fmt.Errorf("files cannot be removed from the nightmarket remote (by design)")
}

func Init() annexremote.Helper {
	h := &helper{
		KeyLocksLock: sync.Mutex{},
		KeyLocksCond: sync.Cond{},
		KeyLocks:     map[string]void{},
	}
	h.KeyLocksCond.L = &h.KeyLocksLock
	return h
}
