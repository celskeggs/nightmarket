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

func Prompter(in io.Reader, out io.Writer) func(string) (string, error) {
	reader := ReadLines(in)
	return func(prompt string) (string, error) {
		_, err := out.Write([]byte(prompt))
		if err == io.EOF {
			return "", errors.New("EOF on input")
		} else if err != nil {
			return "", err
		}
		return reader()
	}
}
