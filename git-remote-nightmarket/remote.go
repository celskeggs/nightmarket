package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/celskeggs/nightmarket/git-remote-nightmarket/cryptapi"
	"github.com/celskeggs/nightmarket/git-remote-nightmarket/gitremote"
)

const MergeDevice = "latest"
const BranchPrefix = "refs/heads/"
const SpecialAnnexPrefix = "synced/"
const SpecialAnnexPath = "synced/git-annex"
const Version = 1

func DecodePseudoRef(ref string) (device, branch string, err error) {
	if err := gitremote.PartiallyValidateRefName(ref); err != nil {
		return "", "", err
	}
	if !strings.HasPrefix(ref, BranchPrefix) {
		return "", "", fmt.Errorf("invalid remote ref: %q", ref)
	}
	ref = ref[len(BranchPrefix):]
	if ref == SpecialAnnexPath {
		return MergeDevice, SpecialAnnexPath, nil
	} else if strings.HasPrefix(ref, SpecialAnnexPrefix) {
		ref = ref[len(SpecialAnnexPrefix):]
		parts := strings.SplitN(ref, "/", 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid remote ref: %q", ref)
		}
		// basically, transpose "synced/latest/main" into "latest/synced/main"
		return parts[0], SpecialAnnexPrefix + parts[1], nil
	} else {
		parts := strings.SplitN(ref, "/", 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid remote ref: %q", ref)
		}
		return parts[0], parts[1], nil
	}
}

func EncodePseudoRef(device, branch string) (string, error) {
	if device == MergeDevice && branch == SpecialAnnexPath {
		return BranchPrefix + SpecialAnnexPath, nil
	}
	if err := gitremote.PartiallyValidateRefName(device); err != nil {
		return "", err
	}
	if err := gitremote.PartiallyValidateRefName(branch); err != nil {
		return "", err
	}
	if strings.Contains(device, "/") {
		return "", fmt.Errorf("invalid device name: %q", device)
	}
	if strings.HasPrefix(branch, SpecialAnnexPrefix) {
		branch = branch[len(SpecialAnnexPrefix):]
		return BranchPrefix + SpecialAnnexPrefix + device + "/" + branch, nil
	}
	return BranchPrefix + device + "/" + branch, nil
}

func DecodeInfix(infix string) (deviceIndex, globalIndex uint64, err error) {
	parts := strings.Split(infix, "-")
	if len(parts) != 3 || parts[0] != "push" {
		return 0, 0, fmt.Errorf("invalid filename infix %q", infix)
	}
	deviceIndex, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	globalIndex, err = strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return deviceIndex, globalIndex, nil
}

func EncodeInfix(deviceIndex, globalIndex uint64) string {
	return fmt.Sprintf("push-%d-%d", deviceIndex, globalIndex)
}

type PackHeader struct {
	Version int `json:"version"`
	// branch -> sha1
	Branches map[string]string `json:"branches"`
}

type RefDBState struct {
	// device -> (branch -> sha1)
	DeviceBranches map[string]map[string]string
	// list of filenames that have already been downloaded and unpacked
	MergedPacks []string
}

type NightmarketHelper struct {
	Clerk  *cryptapi.Clerk
	GitDir string
	Remote string
	RefDB  *RefDBState
}

func InitHelper(remote string, configPath string) (gitremote.Helper, error) {
	fi, err := os.Stat(configPath)
	if err != nil {
		return nil, err
	}
	if (fi.Mode() & os.ModePerm) != 0o600 {
		return nil, fmt.Errorf(
			"configuration %q is not protected from other users: chmod it to 0600 for safety", configPath)
	}
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var config cryptapi.ClerkConfig
	if err = json.Unmarshal(configData, &config); err != nil {
		return nil, err
	}
	gitDir := os.Getenv("GIT_DIR")
	if len(gitDir) == 0 {
		return nil, errors.New("no GIT_DIR specified")
	}
	_, err = os.ReadDir(gitDir)
	if err != nil {
		return nil, errors.New("cannot access GIT_DIR")
	}
	nm := &NightmarketHelper{
		Clerk:  cryptapi.NewClerk(config),
		GitDir: gitDir,
		Remote: remote,
		RefDB:  nil,
	}
	return nm, nil
}

func (n *NightmarketHelper) refDBPath(temp bool) string {
	var tempInfix string
	if temp {
		tempInfix = ".temp"
	}
	return path.Join(n.GitDir, fmt.Sprintf("nightmarket-%s-cache%s.json", n.Remote, tempInfix))
}

// TODO: introduce some sort of locking for the case of parallel fetches... though the worst case scenario is probably
// just that we redownload a particular packfile.
func (n *NightmarketHelper) loadRefDB() error {
	refData, err := ioutil.ReadFile(n.refDBPath(false))
	if errors.Is(err, fs.ErrNotExist) {
		// just in case we crashed halfway through the update
		refData, err = ioutil.ReadFile(n.refDBPath(true))
	}
	if err != nil {
		return fmt.Errorf("while loading refdb: %w", err)
	}
	c := &RefDBState{}
	if err = json.Unmarshal(refData, c); err != nil {
		return err
	}
	n.RefDB = c
	return nil
}

func (n *NightmarketHelper) saveRefDB() error {
	if n.RefDB == nil {
		return fmt.Errorf("internal error: cache state should not have been nil")
	}
	refData, err := json.Marshal(*n.RefDB)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(n.refDBPath(true), refData, 0666)
	if err != nil {
		return err
	}
	if err = os.Remove(n.refDBPath(false)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err = os.Rename(n.refDBPath(true), n.refDBPath(false)); err != nil {
		return err
	}
	return nil
}

type void struct{}

func (n *NightmarketHelper) listDownloads() ([]string, error) {
	objects, err := n.Clerk.ListObjects()
	if err != nil {
		return nil, err
	}
	toDownload := map[string]void{}
	for _, object := range objects {
		toDownload[object] = void{}
	}
	for _, pack := range n.RefDB.MergedPacks {
		if _, found := toDownload[pack]; !found {
			return nil, fmt.Errorf("the pack %q that we previously downloaded is gone", pack)
		}
		delete(toDownload, pack)
	}
	var orderedDownloads []string
	indexLookup := map[string]uint64{}
	for download := range toDownload {
		orderedDownloads = append(orderedDownloads, download)
		// validate that infix can be extracted
		_, infix, _, err := cryptapi.SplitPath(download)
		if err != nil {
			return nil, err
		}
		_, globalIndex, err := DecodeInfix(infix)
		if err != nil {
			return nil, err
		}
		indexLookup[download] = globalIndex
	}
	sort.Slice(orderedDownloads, func(i, j int) bool {
		indexI, okI := indexLookup[orderedDownloads[i]]
		indexJ, okJ := indexLookup[orderedDownloads[j]]
		if !okI || !okJ {
			panic("internal error: should have found this index")
		}
		return indexI < indexJ
	})
	return orderedDownloads, nil
}

func (n *NightmarketHelper) downloadAndUnpack(packPath string) (h *PackHeader, err error) {
	_, _ = fmt.Fprintf(os.Stderr, "nightmarket: downloading and unpacking %q\n", packPath)
	rc, err := n.Clerk.GetDecryptObjectStream(packPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err2 := rc.Close(); err2 != nil {
			err = multierror.Append(err, err2)
		}
	}()
	// use a buffered reader to strip off the first line (which contains the JSON header)
	buf := bufio.NewReader(rc)
	headerBytes, err := buf.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var header PackHeader
	if err = json.Unmarshal(headerBytes, &header); err != nil {
		return nil, err
	}
	if header.Version != Version {
		return nil, fmt.Errorf("version mismatch: %d instead of %d", header.Version, Version)
	}
	// now feed the rest of the file after the header into git unpack-objects
	cmd := exec.Command("git", "unpack-objects", "-q")
	cmd.Stdin = buf
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	if len(output) != 0 {
		return nil, fmt.Errorf("unexpected output from unpack-objects: %q", string(output))
	}
	return &header, nil
}

func (n *NightmarketHelper) gitObjectType(sha1 string) (string, error) {
	output, err := exec.Command("git", "cat-file", "-t", "--", sha1).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func (n *NightmarketHelper) gitRevParse(ref string) (string, error) {
	output, err := exec.Command("git", "rev-parse", "--verify", "--end-of-options", ref).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func (n *NightmarketHelper) gitIsAncestor(ancestor, descendant string) (bool, error) {
	output, err := exec.Command("git", "merge-base", "--is-ancestor", "--", ancestor, descendant).Output()
	if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 1 && len(ee.Stderr) == 0 && len(output) == 0 {
		return false, nil
	} else if err != nil {
		return false, err
	} else if len(output) != 0 {
		return false, fmt.Errorf("unexpected output from merge-base: %q", string(output))
	} else {
		// if error code is 0, and there's no output, then it's an ancestor!
		return true, nil
	}
}

func (n *NightmarketHelper) updateFromHeader(device string, packPath string, header *PackHeader) error {
	rf := n.RefDB
	if rf == nil {
		return errors.New("internal error: RefDB should not be nil at this point")
	}
	if device == MergeDevice {
		return errors.New("invalid device name")
	}
	rf.MergedPacks = append(rf.MergedPacks, packPath)
	if rf.DeviceBranches[device] == nil {
		rf.DeviceBranches[device] = map[string]string{}
	}
	branches := rf.DeviceBranches[device]
	for branch, sha1 := range header.Branches {
		branches[branch] = sha1
	}
	if err := n.saveRefDB(); err != nil {
		return err
	}
	return nil
}

func (n *NightmarketHelper) synch() error {
	err := n.loadRefDB()
	if errors.Is(err, fs.ErrNotExist) {
		_, _ = fmt.Fprintf(os.Stderr, "nightmarket: initializing new local refdb\n")
		n.RefDB = &RefDBState{
			DeviceBranches: map[string]map[string]string{},
			MergedPacks:    nil,
		}
	} else if err != nil {
		return err
	}
	toDownload, err := n.listDownloads()
	if err != nil {
		return err
	}
	for _, packPath := range toDownload {
		device, _, _, err := cryptapi.SplitPath(packPath)
		if err != nil {
			return err
		}
		header, err := n.downloadAndUnpack(packPath)
		if err != nil {
			return err
		}
		if err = n.updateFromHeader(device, packPath, header); err != nil {
			return err
		}
	}
	if len(n.RefDB.MergedPacks) == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "nightmarket: remote is empty; ignoring\n")
	}
	return nil
}

// mergeCommits returns an empty string if the commits are disputed, or the latest commit if no dispute exists
func (n *NightmarketHelper) mergeCommits(sha1s []string) (string, error) {
	proposed := sha1s[0]
	for _, commit := range sha1s[1:] {
		if isDescendant, err := n.gitIsAncestor(proposed, commit); err != nil {
			return "", err
		} else if isDescendant {
			// our new commit is a descendant of the old commit, so it takes precedence
			proposed = commit
			continue
		}
		if isAncestor, err := n.gitIsAncestor(commit, proposed); err != nil {
			return "", err
		} else if isAncestor {
			// our new commit is an ancestor of the old commit, so the old commit takes precedence
			continue
		}
		// otherwise, the new commit is neither an ancestor or descendant of the old commit... this branch is disputed!
		return "", nil
	}
	return proposed, nil
}

func (n *NightmarketHelper) List() ([]gitremote.ListRef, error) {
	if err := n.synch(); err != nil {
		return nil, err
	}
	var allRefs []gitremote.ListRef
	competitors := map[string][]string{}
	for device, refs := range n.RefDB.DeviceBranches {
		if device == MergeDevice {
			return nil, errors.New("unexpectedly encountered merge device in branches list")
		}
		for branch, sha1 := range refs {
			ref, err := EncodePseudoRef(device, branch)
			if err != nil {
				return nil, err
			}
			allRefs = append(allRefs, gitremote.ListRef{
				Sha1: sha1,
				Name: ref,
			})
			mergeRef, err := EncodePseudoRef(MergeDevice, branch)
			if err != nil {
				return nil, err
			}
			competitors[mergeRef] = append(competitors[mergeRef], sha1)
		}
	}
	headRef, err := EncodePseudoRef(MergeDevice, "main")
	if err != nil {
		return nil, err
	}
	var hasHead bool
	for mergeRef, sha1s := range competitors {
		mergeSha1, err := n.mergeCommits(sha1s)
		if err != nil {
			return nil, err
		}
		if mergeSha1 == "" {
			_, _ = fmt.Fprintf(os.Stderr, "nightmarket: removing disputed branch %q from latest\n", mergeRef)
			continue
		}
		allRefs = append(allRefs, gitremote.ListRef{
			Sha1: mergeSha1,
			Name: mergeRef,
		})
		if mergeRef == headRef {
			hasHead = true
		}
	}
	sort.Slice(allRefs, func(i, j int) bool {
		return allRefs[i].Name < allRefs[j].Name
	})
	if hasHead {
		allRefs = append(allRefs, gitremote.ListRef{
			Sha1: "@" + headRef,
			Name: "HEAD",
		})
	}
	return allRefs, nil
}

func (n *NightmarketHelper) ListForPush() ([]gitremote.ListRef, error) {
	return n.List()
}

func (n *NightmarketHelper) Fetch(refs []gitremote.FetchRef) error {
	rf := n.RefDB
	if rf == nil {
		return errors.New("list required before fetch")
	}
	// all fetches have actually already been performed during list, so just make sure it's all okay
	for _, ref := range refs {
		device, branch, err := DecodePseudoRef(ref.Name)
		if err != nil {
			return err
		}
		var acceptable bool
		if device == MergeDevice {
			for _, branches := range rf.DeviceBranches {
				// note: this is approximate, because it allows fetches to have slightly the wrong sha1... but this is
				// really only for consistency checking, so that's fine.
				if sha1, found := branches[branch]; found && sha1 == ref.Sha1 {
					acceptable = true
				}
			}
		} else {
			if sha1, found := rf.DeviceBranches[device][branch]; found && sha1 == ref.Sha1 {
				acceptable = true
			}
		}
		if !acceptable {
			return fmt.Errorf("requested ref not found in refdb: %q -> %q", ref.Name, ref.Sha1)
		}
		objectType, err := n.gitObjectType(ref.Sha1)
		if err != nil {
			return err
		}
		if objectType != "commit" {
			return fmt.Errorf("did not find expected unpacked object: %q instead of commit", objectType)
		}
	}
	return nil
}

func (n *NightmarketHelper) nextPackName(deviceName string) (string, error) {
	var nextDeviceIndex uint64
	var nextGlobalIndex uint64
	observed := map[uint64]void{}
	for _, name := range n.RefDB.MergedPacks {
		device, infix, _, err := cryptapi.SplitPath(name)
		if err != nil {
			return "", err
		}
		deviceIndex, globalIndex, err := DecodeInfix(infix)
		if err != nil {
			return "", err
		}
		if device == deviceName {
			if deviceIndex >= nextDeviceIndex {
				nextDeviceIndex = deviceIndex + 1
			}
			if _, alreadyFound := observed[deviceIndex]; alreadyFound {
				return "", fmt.Errorf("duplicate pack previously pushed with sequence number %d", deviceIndex)
			}
			observed[deviceIndex] = void{}
		}
		if globalIndex >= nextGlobalIndex {
			nextGlobalIndex = globalIndex + 1
		}
	}
	// ensure numbers are contiguous
	for i := uint64(0); i < nextDeviceIndex; i++ {
		if _, found := observed[i]; !found {
			return "", fmt.Errorf("non-contiguous sequence numbers detected: %v", observed)
		}
	}
	return EncodeInfix(nextDeviceIndex, nextGlobalIndex), nil
}

type CountWriter struct {
	Length int64
}

func (c *CountWriter) Write(p []byte) (int, error) {
	c.Length += int64(len(p))
	return len(p), nil
}

var ErrEmptyPack = errors.New("empty pack; do not upload")

func (n *NightmarketHelper) Push(refs []gitremote.PushRef) ([]error, error) {
	deviceName, err := n.Clerk.DeviceName()
	if err != nil {
		return nil, err
	}
	header, packPlan, err := n.preparePush(deviceName, refs)
	if err != nil {
		return nil, err
	}
	infix, err := n.nextPackName(deviceName)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	encodeDone := make(chan void)
	go func() {
		defer close(encodeDone)
		var encodeErr error
		defer func() {
			_ = pw.CloseWithError(encodeErr)
		}()
		encodeErr = json.NewEncoder(pw).Encode(header)
		if encodeErr != nil {
			return
		}
		cw := &CountWriter{}
		cmd := exec.Command("git", "pack-objects", "--stdout", "--thin", "--revs", "--non-empty")
		cmd.Stdout = io.MultiWriter(pw, cw)
		cmd.Stdin = strings.NewReader(packPlan)
		cmd.Stderr = os.Stderr
		encodeErr = cmd.Run()
		if encodeErr != nil {
			return
		}
		if cw.Length == 0 {
			encodeErr = ErrEmptyPack
		}
	}()
	defer func() {
		_ = pr.Close()
		<-encodeDone
	}()
	createdFilename, err := n.Clerk.PutEncryptObjectStream(infix, pr)
	if err == ErrEmptyPack {
		// TODO: is this the right way to handle this condition? it prevents us from updating refs without new objects!
		_, _ = fmt.Fprintf(os.Stderr, "Not uploading a new pack; no new data to transmit.\n")
		// return no errors!
		return make([]error, len(refs)), nil
	}
	if err != nil {
		return nil, err
	}
	if len(createdFilename) == 0 {
		return nil, errors.New("invalid empty created filename")
	}
	// mark this as merged so we don't immediately go redownload our own upload
	if err = n.updateFromHeader(deviceName, createdFilename, header); err != nil {
		return nil, err
	}
	// upload complete; return no errors!
	return make([]error, len(refs)), nil
}

func (n *NightmarketHelper) preparePush(deviceName string, refs []gitremote.PushRef) (*PackHeader, string, error) {
	rf := n.RefDB
	if rf == nil {
		return nil, "", errors.New("list required before push")
	}
	branches := map[string]string{}
	var packPlan strings.Builder
	for _, ref := range refs {
		// validate and extract branch info
		device, branch, err := DecodePseudoRef(ref.Dest)
		if err != nil {
			return nil, "", err
		}
		if device == MergeDevice {
			// if we push to the merged namespace, rewrite so we're actually pushing to our own namespace
			device = deviceName
			// TODO: it may be worth detecting the case where we passively do a force-push in this case, because
			// otherwise we may not detect a force-push when there is a conflict and the latest/ branch was disputed
			// and not surfaced to Git
		} else if device != deviceName {
			return nil, "", fmt.Errorf("attempt to push to branch %q (%q %q) from device %q",
				ref.Dest, device, branch, deviceName)
		}
		commitHash, err := n.gitRevParse(ref.Source)
		if err != nil {
			return nil, "", err
		}
		previousHash, found := branches[branch]
		if found {
			isAncestor, err := n.gitIsAncestor(previousHash, commitHash)
			if err != nil {
				return nil, "", err
			}
			if !isAncestor {
				if ref.Force {
					_, _ = fmt.Fprintf(os.Stderr, "nightmarket: rewinding history during force-push to %q\n", branch)
					// but don't error out, because they DID specify a force-push
				} else {
					return nil, "", fmt.Errorf(
						"non-force push %q from %q to %q would have rewound history", branch, previousHash, commitHash)
				}
			}
		}
		// add to branch list
		branches[branch] = commitHash
		// and add to pack plan
		if _, err = fmt.Fprintln(&packPlan, commitHash); err != nil {
			return nil, "", err
		}
	}
	// add all known sha1s as exclusions to the pack plan, so we don't upload data already uploaded previously
	knownLookup := map[string]void{}
	for _, branchesOnDevice := range rf.DeviceBranches {
		for _, sha1 := range branchesOnDevice {
			if _, found := knownLookup[sha1]; !found {
				knownLookup[sha1] = void{}
				if _, err := fmt.Fprintf(&packPlan, "^%s\n", sha1); err != nil {
					return nil, "", err
				}
			}
		}
	}
	return &PackHeader{
		Version:  Version,
		Branches: branches,
	}, packPlan.String(), nil
}

func main() {
	gitremote.Mainloop(InitHelper)
}
