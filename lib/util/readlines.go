package util

import (
	"bufio"
	"errors"
	"io"
)

func ReadLines(in io.Reader) func() (string, error) {
	reader := bufio.NewReader(in)
	return func() (string, error) {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		if len(line) == 0 || line[len(line)-1] != '\n' {
			return "", errors.New("invalid ReadString behavior")
		}
		return line[:len(line)-1], nil
	}
}
