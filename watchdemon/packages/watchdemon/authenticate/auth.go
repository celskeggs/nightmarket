package main

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/alexedwards/argon2id"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Reply struct {
	URL      string      `json:"url"`
	Headers  http.Header `json:"headers"`
	Filename string      `json:"created-filename,omitempty"`
}

type ReplyError struct {
	Error string `json:"error"`
}

func response(data interface{}) map[string]interface{} {
	encoded, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return map[string]interface{}{
		"headers": map[string]string{"Content-Type": "application/json"},
		"body":    string(encoded),
	}
}

func Main(in map[string]interface{}) (out map[string]interface{}) {
	device, ok1 := in["device"].(string)
	token, ok2 := in["token"].(string)
	mode, ok3 := in["mode"].(string)
	key, ok4 := in["key"].(string)
	if !ok1 || len(device) == 0 || !ok2 || len(token) == 0 || !ok3 || len(mode) == 0 || !ok4 {
		return response(ReplyError{"invalid parameters"})
	}
	authorizedTokensStr := os.Getenv("WATCHDEMON_AUTHORIZED")
	if len(authorizedTokensStr) == 0 {
		return response(ReplyError{"no authorization configuration"})
	}
	// map from device name to token
	authorizedTokens := map[string]string{}
	err := json.Unmarshal([]byte(authorizedTokensStr), &authorizedTokens)
	if err != nil {
		return response(ReplyError{err.Error()})
	}
	authorized := authorizedTokens[device]
	if authorized == "" {
		return response(ReplyError{"no such device"})
	}
	match, err := argon2id.ComparePasswordAndHash(token, authorized)
	if err != nil {
		return response(ReplyError{err.Error()})
	}
	if !match {
		return response(ReplyError{"not authorized"})
	}
	spaceEndpoint := os.Getenv("WATCHDEMON_SPACE_ENDPOINT")
	spaceName := os.Getenv("WATCHDEMON_SPACE_NAME")
	spaceRegion := os.Getenv("WATCHDEMON_SPACE_REGION")
	accessKey := os.Getenv("WATCHDEMON_ACCESS_KEY")
	secretKey := os.Getenv("WATCHDEMON_SECRET_KEY")
	if len(accessKey) == 0 || len(secretKey) == 0 {
		return response(ReplyError{"missing access or secret key"})
	}
	var r Reply
	spacesSession, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Endpoint:    aws.String(spaceEndpoint),
		Region:      aws.String(spaceRegion),
	})
	if err != nil {
		return response(ReplyError{"spaces error: " + err.Error()})
	}
	api := s3.New(spacesSession)
	var req *request.Request
	switch mode {
	case "List":
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(spaceName),
		}
		if len(key) != 0 {
			input.ContinuationToken = aws.String(key)
		}
		req, _ = api.ListObjectsV2Request(input)
	case "Get":
		if len(key) == 0 {
			return response(ReplyError{"no key specified"})
		}
		req, _ = api.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(spaceName),
			Key:    aws.String(key),
		})
	case "Put":
		sha256, okSHA256 := in["sha256"].(string)
		if len(key) == 0 || !okSHA256 || len(sha256) != 64 {
			return response(ReplyError{"either no key or no hash specified"})
		}
		// make sure it's a valid sha256 string
		_, err := hex.DecodeString(sha256)
		if err != nil {
			return response(ReplyError{err.Error()})
		}
		filename := device + "/" + key + "#" + sha256
		req, _ = api.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String(spaceName),
			// checksum is included in filename because the underlying API won't prevent overwriting
			Key: aws.String(filename),
		})
		r.Filename = filename
		// checksum is required to prevent user from substituting a different version of the file
		req.HTTPRequest.Header.Set("X-Amz-Content-Sha256", sha256)
	default:
		return response(ReplyError{"invalid request mode"})
	}
	r.URL, r.Headers, err = req.PresignRequest(time.Second * 10)
	if err != nil {
		return response(ReplyError{"presign error: " + err.Error()})
	}
	return response(r)
}
