package gitremote

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/celskeggs/nightmarket/lib/util"
)

type ListRef struct {
	// TODO: support for listing other forms of refs besides sha1 hashes
	Sha1 string
	Name string
}

type FetchRef struct {
	Sha1 string
	Name string
}

type PushRef struct {
	Force  bool
	Source string
	Dest   string
}

type Helper interface {
	List() ([]ListRef, error)
	ListForPush() ([]ListRef, error)
	Fetch(refs []FetchRef) error
	Push(refs []PushRef) ([]error, error)
	//Close() error
}

func isValidSha1(hash string) error {
	if len(hash) != 40 {
		return errors.New("wrong length for a sha1 hash")
	}
	_, err := hex.DecodeString(hash)
	return err
}

func PartiallyValidateRefName(ref string) error {
	// not complete validation... just enough for safety
	for _, c := range []byte(ref) {
		if c <= ' ' || c >= '~' {
			return errors.New("invalid ref name")
		}
	}
	if len(ref) == 0 {
		return errors.New("ref name is too short")
	}
	return nil
}

func printList(out io.StringWriter, refs []ListRef) error {
	for _, ref := range refs {
		if strings.HasPrefix(ref.Sha1, "@") {
			if err := PartiallyValidateRefName(ref.Sha1); err != nil {
				return err
			}
			// valid symbolic ref
		} else {
			if err := isValidSha1(ref.Sha1); err != nil {
				return err
			}
			// valid sha1
		}
		if err := PartiallyValidateRefName(ref.Name); err != nil {
			return err
		}
		_, err := out.WriteString(ref.Sha1 + " " + ref.Name + "\n")
		if err != nil {
			return err
		}
	}
	_, err := out.WriteString("\n")
	return err
}

func parseFetchRef(line string) (FetchRef, error) {
	parts := strings.Split(line, " ")
	if len(parts) != 3 || parts[0] != "fetch" {
		return FetchRef{}, fmt.Errorf("invalid fetch line: %q", line)
	}
	return FetchRef{
		Sha1: parts[1],
		Name: parts[2],
	}, nil
}

func parsePushRef(line string) (PushRef, error) {
	if !strings.HasPrefix(line, "push ") || len(line) < 8 {
		return PushRef{}, fmt.Errorf("invalid fetch line: %q", line)
	}
	line = line[5:]
	forcePush := strings.HasPrefix(line, "+")
	if forcePush {
		line = line[1:]
	}
	parts := strings.Split(line, ":")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		return PushRef{}, fmt.Errorf("invalid fetch line: %q", line)
	}
	return PushRef{
		Force:  forcePush,
		Source: parts[0],
		Dest:   parts[1],
	}, nil
}

func mainloop(in io.Reader, out io.StringWriter, helper Helper) (eo error) {
	reader := util.ReadLines(in)
	for {
		line, err := reader()
		if err != nil {
			return err
		}
		switch {
		case line == "":
			// end of command stream
			return nil
		case line == "capabilities":
			_, err := out.WriteString("fetch\npush\n\n")
			if err != nil {
				return err
			}
		case line == "list":
			list, err := helper.List()
			if err != nil {
				return err
			}
			if err := printList(out, list); err != nil {
				return err
			}
		case line == "list for-push":
			list, err := helper.ListForPush()
			if err != nil {
				return err
			}
			if err := printList(out, list); err != nil {
				return err
			}
		case strings.HasPrefix(line, "fetch "):
			var refs []FetchRef
			for line != "" {
				ref, err := parseFetchRef(line)
				if err != nil {
					return err
				}
				refs = append(refs, ref)
				if line, err = reader(); err != nil {
					return err
				}
			}
			if err := helper.Fetch(refs); err != nil {
				return err
			}
			_, err := out.WriteString("\n")
			if err != nil {
				return err
			}
		case strings.HasPrefix(line, "push "):
			var refs []PushRef
			for line != "" {
				ref, err := parsePushRef(line)
				if err != nil {
					return err
				}
				refs = append(refs, ref)
				if line, err = reader(); err != nil {
					return err
				}
			}
			statuses, err := helper.Push(refs)
			if err != nil {
				return err
			}
			if len(statuses) != len(refs) {
				return errors.New("remote helper returned wrong number of statuses for push")
			}
			for i, status := range statuses {
				if status == nil {
					_, err = out.WriteString("ok " + refs[i].Dest + "\n")
				} else {
					_, err = out.WriteString(fmt.Sprintf("error %s %q\n", refs[i].Dest, status.Error()))
				}
				if err != nil {
					return err
				}
			}
			_, err = out.WriteString("\n")
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognized input line %q", line)
		}
	}
}

func Mainloop(init func(remote, url string) (Helper, error)) {
	if len(os.Args) != 3 {
		_, _ = fmt.Fprintf(os.Stderr, "%s expected two arguments\n", os.Args[0])
		os.Exit(1)
	}
	helper, err := init(os.Args[1], os.Args[2])
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s init error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
	err = mainloop(os.Stdin, os.Stdout, helper)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s loop error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}
