package annexhelper

import (
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

const LockDebug = false

const resyncStartDelay = 10 * time.Second
const resyncPauseDelay = 30 * time.Second

type helper struct {
	ClerkLock  sync.Mutex
	ClerkMaybe *cryptapi.Clerk

	// note: objectlist may become stale, so recheck if it really matters!
	ObjectLock       sync.Mutex
	ObjectMapLocked  map[string]ObjectMetadata
	LastUpdateLocked time.Time
	Syncher          *syncher

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

type ObjectMetadata struct {
	Error      error
	ObjectPath string
}

func generateObjectMap(objects []string) (map[string]ObjectMetadata, error) {
	objMap := map[string]ObjectMetadata{}
	for _, objectPath := range objects {
		_, infix, _, err := cryptapi.SplitPath(objectPath)
		if err != nil {
			return nil, err
		}
		om := ObjectMetadata{
			ObjectPath: objectPath,
		}
		if oldMeta, found := objMap[infix]; found {
			om.Error = fmt.Errorf(
				"detected duplicate files for infix %q: %q and %q", infix, oldMeta.ObjectPath, objectPath)
		}
		objMap[infix] = om
	}
	return objMap, nil
}

type synchResult struct {
	ObjectMap  map[string]ObjectMetadata
	UpdateTime time.Time
	Error      error
}

type syncher struct {
	StateLock  sync.Mutex
	StateCond  sync.Cond
	Started    bool
	Completion *synchResult
}

func newSyncher(clerk *cryptapi.Clerk) *syncher {
	s := &syncher{}
	s.StateCond.L = &s.StateLock
	go func() {
		s.postComplete(nil)
		for {
			result := &synchResult{}
			objects, err := clerk.ListObjects()
			if err == nil {
				objMap, err := generateObjectMap(objects)
				if err == nil {
					result.ObjectMap = objMap
				}
			}
			result.Error = err
			result.UpdateTime = time.Now()
			s.postComplete(result)
		}
	}()
	return s
}

func (s *syncher) postComplete(result *synchResult) {
	s.StateLock.Lock()
	defer s.StateLock.Unlock()
	if result != nil {
		if !s.Started || s.Completion != nil {
			panic("invalid state")
		}
		s.Completion = result
		s.StateCond.Broadcast()
	}
	// wait until we have work to do
	for !s.Started || s.Completion != nil {
		s.StateCond.Wait()
	}
}

func (s *syncher) Start() {
	s.StateLock.Lock()
	defer s.StateLock.Unlock()
	s.Started = true
	s.StateCond.Broadcast()
}

func (s *syncher) Wait() {
	s.StateLock.Lock()
	defer s.StateLock.Unlock()
	if !s.Started {
		panic("invalid state")
	}
	for s.Completion == nil {
		s.StateCond.Wait()
	}
}

func (s *syncher) CheckUpdate() (bool, map[string]ObjectMetadata, time.Time, error) {
	s.StateLock.Lock()
	defer s.StateLock.Unlock()
	if s.Completion != nil {
		c := s.Completion
		s.Started, s.Completion = false, nil
		return true, c.ObjectMap, c.UpdateTime, c.Error
	}
	return false, nil, time.Time{}, nil
}

func (h *helper) syncList() error {
	h.ObjectLock.Lock()
	defer h.ObjectLock.Unlock()
	return h.syncListLocked()
}

func (h *helper) syncListLocked() error {
	if h.LastUpdateLocked.IsZero() || time.Now().After(h.LastUpdateLocked.Add(resyncPauseDelay)) {
		if h.Syncher == nil {
			clerk, err := h.getClerk()
			if err != nil {
				return err
			}
			h.Syncher = newSyncher(clerk)
		}
		h.Syncher.Start()
		h.Syncher.Wait()
	}
	updated, objMap, updateTime, err := h.Syncher.CheckUpdate()
	if err != nil {
		return err
	}
	if updated {
		h.ObjectMapLocked = objMap
		h.LastUpdateLocked = updateTime
	}
	if time.Now().After(h.LastUpdateLocked.Add(resyncStartDelay)) {
		h.Syncher.Start()
	}
	return nil
}

func (h *helper) addObjectToList(objectPath string) error {
	_, infix, _, err := cryptapi.SplitPath(objectPath)
	if err != nil {
		return err
	}
	h.ObjectLock.Lock()
	defer h.ObjectLock.Unlock()
	if metadata, found := h.ObjectMapLocked[infix]; found {
		if metadata.ObjectPath == objectPath {
			// this is caused by the synchronization thread noticing we added this before we report it. it's fine.
			return nil
		} else {
			// but if the object paths DON'T match, then we actually have two different uploads!
			return fmt.Errorf("attempt to add duplicate object to list: infix %q", infix)
		}
	}
	h.ObjectMapLocked[infix] = ObjectMetadata{
		ObjectPath: objectPath,
	}
	return nil
}

func (h *helper) getObjectMetadata(infix string) (ObjectMetadata, bool, error) {
	h.ObjectLock.Lock()
	defer h.ObjectLock.Unlock()
	metadata, found := h.ObjectMapLocked[infix]
	if !found {
		if err := h.syncListLocked(); err != nil {
			return ObjectMetadata{}, false, err
		}
		metadata, found = h.ObjectMapLocked[infix]
	}
	return metadata, found, nil
}

func (h *helper) Prepare(a *annexremote.Responder) error {
	if err := h.prepareClerk(a); err != nil {
		return err
	}
	if err := h.syncList(); err != nil {
		return err
	}
	return nil
}

func (h *helper) lockKey(a *annexremote.Responder, key string) {
	if LockDebug {
		defer func() {
			if err := a.Debug("Locked key: " + key); err != nil {
				panic(err)
			}
		}()
	}
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

func (h *helper) unlockKey(a *annexremote.Responder, key string) {
	if LockDebug {
		if err := a.Debug("Unlocked key: " + key); err != nil {
			panic(err)
		}
	}
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

func (h *helper) locateFile(key string) (path string, err error) {
	clerk, err := h.getClerk()
	if err != nil {
		return "", err
	}
	cryptedFilename := keyToInfix(clerk, key)
	// first do a check to see if we've already located the file, without any network traffic
	metadata, found, err := h.getObjectMetadata(cryptedFilename)
	if err != nil {
		return "", err
	}
	if !found {
		// not found
		return "", nil
	}
	if metadata.Error != nil {
		// there's an issue with the object, so report that now
		return "", metadata.Error
	}
	if metadata.ObjectPath == "" {
		panic("invalid object path")
	}
	// found something!
	return metadata.ObjectPath, nil
}

func (h *helper) TransferRetrieve(a *annexremote.Responder, key string, tempfilepath string) (err error) {
	h.lockKey(a, key)
	defer h.unlockKey(a, key)

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
	h.lockKey(a, key)
	defer h.unlockKey(a, key)

	path, err := h.locateFile(key)
	if err != nil {
		return false, err
	}
	return path != "", nil
}

func (h *helper) TransferStore(a *annexremote.Responder, key string, tempfilepath string) (err error) {
	h.lockKey(a, key)
	defer h.unlockKey(a, key)

	clerk, err := h.getClerk()
	if err != nil {
		return err
	}
	path, err := h.locateFile(key)
	if err != nil {
		return err
	}
	if path != "" {
		// already exists on the remote! no need to upload!
		return nil
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
	return h.addObjectToList(newPath)
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
