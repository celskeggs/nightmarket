package demonapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil"
	"github.com/aws/aws-sdk-go/service/s3"
)

type ClerkConfig struct {
	URL         string `json:"url"`
	SpacePrefix string `json:"prefix"`
	DeviceName  string `json:"device"`
	DeviceToken string `json:"token"`
}

const (
	ModeList = "List"
	ModeGet  = "Get"
	ModePut  = "Put"
)

type Clerk struct {
	Client http.Client
	Config ClerkConfig
}

func (c *Clerk) authenticate(mode, key, checksum string) (string, http.Header, string, error) {
	if len(c.Config.URL) == 0 || len(c.Config.DeviceName) == 0 || len(c.Config.DeviceToken) == 0 || len(c.Config.SpacePrefix) == 0 {
		return "", nil, "", errors.New("missing configuration")
	}
	if !strings.HasPrefix(c.Config.URL, "https://") {
		return "", nil, "", errors.New("URL is not a valid HTTPS URL")
	}
	values := url.Values{
		"device": []string{c.Config.DeviceName},
		"token":  []string{c.Config.DeviceToken},
		"mode":   []string{mode},
		"key":    []string{key},
	}
	if mode == ModePut {
		values["sha256"] = []string{checksum}
	}
	response, err := c.Client.PostForm(c.Config.URL+"/watchdemon/authenticate", values)
	if err != nil {
		return "", nil, "", err
	}
	defer func() { _ = response.Body.Close() }()
	var result map[string]interface{}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return "", nil, "", err
	}
	if str, ok := result["error"].(string); ok {
		return "", nil, "", fmt.Errorf("remote error: %q", str)
	}
	responseURL, ok := result["url"].(string)
	if !ok {
		return "", nil, "", errors.New("no URL returned in JSON object")
	}
	if !strings.HasPrefix(responseURL, c.Config.SpacePrefix) {
		return "", nil, "", errors.New("presigned URL does not match expected pattern")
	}
	headersInterface, ok := result["headers"].(map[string]interface{})
	headers := http.Header{}
	if ok {
		for k, v := range headersInterface {
			vl, ok := v.([]interface{})
			if !ok {
				return "", nil, "", errors.New("invalid header format")
			}
			for _, vi := range vl {
				vis, ok := vi.(string)
				if !ok {
					return "", nil, "", errors.New("invalid header format")
				}
				headers.Add(k, vis)
			}
		}
	}
	var createdFilename string
	if mode == ModePut {
		createdFilename, ok = result["created-filename"].(string)
		if !ok || len(createdFilename) == 0 {
			return "", nil, "", errors.New("invalid created filename")
		}
	}
	return responseURL, headers, createdFilename, nil
}

const PrintTiming = false

func timer(explanation string) func() {
	if PrintTiming {
		start := time.Now()
		return func() {
			_, _ = fmt.Fprintf(os.Stderr, "nightmarket: %s took %v\n", explanation, time.Since(start))
		}
	} else {
		// do nothing
		return func() {}
	}
}

func (c *Clerk) ListObjectsV2(continuationToken *string) (*s3.ListObjectsV2Output, error) {
	defer timer("ListObjectsV2")()
	var contKey string
	if continuationToken != nil {
		if *continuationToken == "" {
			return nil, errors.New("continuation token cannot be empty")
		}
		contKey = *continuationToken
	}
	presignedURL, headers, _, err := c.authenticate(ModeList, contKey, "")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("GET", presignedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header = headers
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("invalid status code %d", resp.StatusCode)
	}
	decoder := xml.NewDecoder(resp.Body)
	result := &s3.ListObjectsV2Output{}
	err = xmlutil.UnmarshalXML(result, decoder, "")
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Clerk) GetObject(path string) ([]byte, error) {
	defer timer("GetObject")()
	stream, err := c.GetObjectStream(path)
	if err != nil {
		return nil, err
	}
	defer func(stream io.ReadCloser) {
		_ = stream.Close()
	}(stream)
	data, err := io.ReadAll(stream)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Clerk) GetObjectStream(path string) (io.ReadCloser, error) {
	defer timer("GetObjectStream")()
	presignedURL, headers, _, err := c.authenticate(ModeGet, path, "")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("GET", presignedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header = headers
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("invalid status code %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// PutObject returns the created filename.
func (c *Clerk) PutObject(pathInfix string, data []byte) (string, error) {
	defer timer("PutObject")()
	checksum := sha256.Sum256(data)
	return c.putObjectInternal(pathInfix, checksum[:], int64(len(data)), bytes.NewReader(data))
}

// Note: this WILL seek the stream to position 0 before beginning
func (c *Clerk) PutObjectStream(pathInfix string, data io.ReadSeeker) (string, error) {
	defer timer("PutObjectStream")()
	if _, err := data.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	hasher := sha256.New()
	length, err := io.Copy(hasher, data)
	if err != nil {
		return "", err
	}
	if _, err := data.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return c.putObjectInternal(pathInfix, hasher.Sum(nil), length, data)
}

func (c *Clerk) putObjectInternal(pathInfix string, sha256sum []byte, length int64, data io.Reader) (string, error) {
	if len(sha256sum) != sha256.Size {
		return "", errors.New("invalid hash")
	}
	checksum := hex.EncodeToString(sha256sum)
	presignedURL, headers, createdFilename, err := c.authenticate(ModePut, pathInfix, checksum)
	if err != nil {
		return "", err
	}
	// data must be wrapped in a NopCloser so that it doesn't get unexpectedly closed
	req, err := http.NewRequest("PUT", presignedURL, io.NopCloser(data))
	if err != nil {
		return "", err
	}
	req.Header = headers
	req.ContentLength = length
	resp, err := c.Client.Do(req)
	if err != nil {
		return "", err
	}
	if err := resp.Body.Close(); err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("invalid status code %d (%q)", resp.StatusCode, resp.Status)
	}
	return createdFilename, nil
}

func (c *Clerk) DeviceName() (string, error) {
	if len(c.Config.DeviceName) == 0 {
		return "", errors.New("invalid device name")
	}
	return c.Config.DeviceName, nil
}
