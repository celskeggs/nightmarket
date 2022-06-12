package cryptapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"filippo.io/age"
	"github.com/celskeggs/nightmarket/git-remote-nightmarket/demonapi"
	"github.com/hashicorp/go-multierror"
)

const Version = 1

type ClerkConfig struct {
	SecretKey   string               `json:"secret-key"`
	SpaceConfig demonapi.ClerkConfig `json:"space"`
}

type Clerk struct {
	RemoteClerk demonapi.Clerk
	Config      ClerkConfig
}

func NewClerk(config ClerkConfig) *Clerk {
	return &Clerk{
		RemoteClerk: demonapi.Clerk{
			Client: http.Client{},
			Config: config.SpaceConfig,
		},
		Config: config,
	}
}

func (c *Clerk) DeviceName() (string, error) {
	return c.RemoteClerk.DeviceName()
}

func (c *Clerk) ListObjects() ([]string, error) {
	objects, err := c.RemoteClerk.ListObjectsV2()
	if err != nil {
		return nil, err
	}
	// TODO: fix so that this function works correctly with pagination
	if *objects.IsTruncated {
		return nil, errors.New("did not return all paths in ListObjects()")
	}
	var paths []string
	for _, object := range objects.Contents {
		paths = append(paths, *object.Key)
	}
	return paths, nil
}

func SplitPath(path string) (device, infix, hash string, e error) {
	s1 := strings.IndexByte(path, '/')
	s2 := strings.LastIndexByte(path, '#')
	if s1 == -1 || s2 == -1 || s2 <= s1 {
		return "", "", "", fmt.Errorf("invalid path: %q", path)
	}
	device, infix, hash = path[0:s1], path[s1+1:s2], path[s2+1:]
	if len(device) == 0 || len(infix) == 0 || len(hash) == 0 {
		return "", "", "", fmt.Errorf("invalid path: %q", path)
	}
	return device, infix, hash, nil
}

type StreamHeader struct {
	Version int    `json:"version"`
	Device  string `json:"device"`
	Infix   string `json:"infix"`
}

func grabHeader(r io.Reader) (*StreamHeader, error) {
	var headerLength uint32
	if err := binary.Read(r, binary.BigEndian, &headerLength); err != nil {
		return nil, err
	}
	data := make([]byte, headerLength)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	var header StreamHeader
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, err
	}
	return &header, nil
}

func writeHeader(wc io.Writer, header StreamHeader) error {
	data, err := json.Marshal(header)
	if err != nil {
		return err
	}
	headerLength := uint32(len(data))
	if err := binary.Write(wc, binary.BigEndian, &headerLength); err != nil {
		return err
	}
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return nil
}

func (c *Clerk) GetDecryptObject(path string) (b []byte, err error) {
	stream, err := c.GetDecryptObjectStream(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err2 := stream.Close(); err2 != nil {
			err = multierror.Append(err, err2)
		}
	}()
	data, err := io.ReadAll(stream)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Clerk) GetDecryptObjectStream(path string) (rc io.ReadCloser, err error) {
	device, infix, hash, err := SplitPath(path)
	if err != nil {
		return nil, err
	}
	identity, err := age.NewScryptIdentity(c.Config.SecretKey)
	if err != nil {
		return nil, err
	}
	stream, err := c.RemoteClerk.GetObjectStream(path)
	if err != nil {
		return nil, err
	}
	hasher := sha256.New()
	bufstream, err := BufferInFile(io.TeeReader(stream, hasher))
	if err != nil {
		return nil, err
	}
	defer func() {
		if rc == nil {
			err = multierror.Append(err, bufstream.Close())
		}
	}()
	realHash := hex.EncodeToString(hasher.Sum(nil))
	if realHash != hash {
		return nil, fmt.Errorf("hash %q did not match downloaded object %q", realHash, path)
	}
	plaintext, err := age.Decrypt(bufstream, identity)
	if err != nil {
		return nil, err
	}
	header, err := grabHeader(plaintext)
	if err != nil {
		return nil, err
	}
	if header.Version != Version {
		return nil, fmt.Errorf("received data contained version=%d instead of version=%d", header.Version, Version)
	}
	if header.Device != device {
		return nil, fmt.Errorf("received data contained device=%q instead of device=%q", header.Device, device)
	}
	if header.Infix != infix {
		return nil, fmt.Errorf("received data contained infix=%q instead of infix=%q", header.Infix, infix)
	}
	// wrap the plaintext reader with the original
	return CombinedReadCloser{
		Reader: plaintext,
		Closer: bufstream,
	}, nil
}

func (c *Clerk) PutEncryptObject(pathInfix string, data []byte) (string, error) {
	return c.PutEncryptObjectStream(pathInfix, bytes.NewReader(data))
}

func (c *Clerk) PutEncryptObjectStream(pathInfix string, data io.Reader) (createdFilename string, err error) {
	recipient, err := age.NewScryptRecipient(c.Config.SecretKey)
	if err != nil {
		return "", err
	}
	f, err := ioutil.TempFile("", "encrypted-object")
	if err != nil {
		return "", err
	}
	defer func() {
		if err2 := f.Close(); err2 != nil {
			err = multierror.Append(err, fmt.Errorf("while closing put-encrypt: %w", err2))
		}
		if err3 := os.Remove(f.Name()); err3 != nil {
			err = multierror.Append(err, err3)
		}
	}()
	wc, err := age.Encrypt(f, recipient)
	if err != nil {
		return "", err
	}
	device, err := c.RemoteClerk.DeviceName()
	if err != nil {
		return "", err
	}
	header := StreamHeader{
		Version: Version,
		Device:  device,
		Infix:   pathInfix,
	}
	if err = writeHeader(wc, header); err != nil {
		return "", err
	}
	if _, err = io.Copy(wc, data); err != nil {
		return "", err
	}
	if err = wc.Close(); err != nil {
		return "", err
	}
	createdFilename, err = c.RemoteClerk.PutObjectStream(pathInfix, f)
	if err != nil {
		return "", err
	}
	return createdFilename, nil
}
