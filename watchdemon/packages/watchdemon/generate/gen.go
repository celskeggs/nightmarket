package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/alexedwards/argon2id"
)

type Request struct {
	Iterations  string
	Megabytes   string
	Parallelism string
}

type Generated struct {
	TimeMs int    `json:"time-ms"`
	Token  string `json:"token"`
	Hash   string `json:"hash"`
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

func Main(in Request) map[string]interface{} {
	var iterations uint32 = 1
	var megabytes uint32 = 1
	var parallelism uint8 = 1
	if len(in.Iterations) > 0 {
		iterations64, err := strconv.ParseUint(in.Iterations, 10, 8)
		if err != nil {
			return response(err.Error())
		}
		iterations = uint32(iterations64)
	}
	if len(in.Megabytes) > 0 {
		megabytes64, err := strconv.ParseUint(in.Megabytes, 10, 8)
		if err != nil {
			return response(err.Error())
		}
		megabytes = uint32(megabytes64)
	}
	if len(in.Parallelism) > 0 {
		parallelism64, err := strconv.ParseUint(in.Parallelism, 10, 8)
		if err != nil {
			return response(err.Error())
		}
		parallelism = uint8(parallelism64)
	}
	var g Generated
	token := make([]byte, 128)
	_, err := rand.Read(token)
	if err != nil {
		return response(err.Error())
	}
	g.Token = base64.StdEncoding.EncodeToString(token)
	start := time.Now()
	g.Hash, err = argon2id.CreateHash(g.Token, &argon2id.Params{
		Memory:      megabytes * 1024, // 96 * 1024,
		Iterations:  iterations,
		Parallelism: parallelism,
		SaltLength:  16,
		KeyLength:   32,
	})
	if err != nil {
		return response(err.Error())
	}
	g.TimeMs = int(time.Now().Sub(start).Milliseconds())
	return response(g)
}
