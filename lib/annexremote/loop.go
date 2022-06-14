package annexremote

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/celskeggs/nightmarket/lib/util"
	"github.com/hashicorp/go-multierror"
)

type GitAnnex struct {
	readLine func() (string, error)
	output   io.StringWriter
}

func (a *GitAnnex) writeLine(line string) error {
	if strings.ContainsRune(line, '\n') {
		return fmt.Errorf("api error: refusing to write line containing interlinear newline: %q", line)
	}
	_, err := a.output.WriteString(line + "\n")
	return err
}

func (a *GitAnnex) Progress(bytes uint64) error {
	return a.writeLine(fmt.Sprint("PROGRESS ", bytes))
}

func (a *GitAnnex) readValue() (string, error) {
	line, err := a.readLine()
	if err == io.EOF {
		return "", io.ErrUnexpectedEOF
	} else if err != nil {
		return "", err
	}
	parts := strings.SplitN(line, " ", 2)
	if len(parts) != 2 || parts[0] != "VALUE" {
		return "", fmt.Errorf("invalid response when expecting VALUE from git-annex: %q", line)
	}
	return parts[1], nil
}

func (a *GitAnnex) DirHash(key string) (string, error) {
	if err := a.writeLine("DIRHASH " + key); err != nil {
		return "", err
	}
	return a.readValue()
}

func (a *GitAnnex) DirHashLower(key string) (string, error) {
	if err := a.writeLine("DIRHASH-LOWER " + key); err != nil {
		return "", err
	}
	return a.readValue()
}

func (a *GitAnnex) SetConfig(setting, value string) error {
	if len(setting) == 0 || strings.ContainsRune(setting, ' ') {
		return fmt.Errorf("api error: refusing to transmit invalid config name %q", setting)
	}
	return a.writeLine("SETCONFIG " + setting + " " + value)
}

func (a *GitAnnex) GetConfig(setting string) (string, error) {
	if err := a.writeLine("GETCONFIG " + setting); err != nil {
		return "", err
	}
	return a.readValue()
}

func (a *GitAnnex) SetCreds(setting, user, password string) error {
	if len(setting) == 0 || strings.ContainsRune(setting, ' ') {
		return fmt.Errorf("api error: refusing to transmit invalid creds name %q", setting)
	}
	if len(user) == 0 || strings.ContainsRune(user, ' ') {
		return fmt.Errorf("api error: refusing to transmit invalid creds username %q", user)
	}
	return a.writeLine("SETCREDS " + setting + " " + user + " " + password)
}

func (a *GitAnnex) GetCreds(setting string) (string, string, error) {
	if err := a.writeLine("GETCREDS " + setting); err != nil {
		return "", "", err
	}
	result, err := a.readLine()
	if err == io.EOF {
		return "", "", io.ErrUnexpectedEOF
	} else if err != nil {
		return "", "", err
	}
	parts := strings.SplitN(result, " ", 3)
	if len(parts) != 3 || parts[0] != "CREDS" {
		return "", "", fmt.Errorf("invalid response when expecting CREDS from git-annex: %q", result)
	}
	return parts[1], parts[2], nil
}

func (a *GitAnnex) GetUUID() (string, error) {
	if err := a.writeLine("GETUUID"); err != nil {
		return "", err
	}
	return a.readValue()
}

func (a *GitAnnex) GetGitDir() (string, error) {
	if err := a.writeLine("GETGITDIR"); err != nil {
		return "", err
	}
	return a.readValue()
}

func (a *GitAnnex) SetState(key, value string) error {
	if len(key) == 0 || strings.ContainsRune(key, ' ') {
		return fmt.Errorf("api error: refusing to transmit invalid state name %q", key)
	}
	return a.writeLine("SETSTATE " + key + " " + value)
}

func (a *GitAnnex) GetState(key string) (string, error) {
	if err := a.writeLine("GETSTATE " + key); err != nil {
		return "", err
	}
	return a.readValue()
}

func (a *GitAnnex) Debug(message string) error {
	return a.writeLine("DEBUG " + message)
}

type Config struct {
	Name        string
	Description string
}

type Helper interface {
	ListConfigs() ([]Config, error)
	Prepare(a *GitAnnex) error
	TransferStore(a *GitAnnex, key string, tempfilepath string) error
	TransferRetrieve(a *GitAnnex, key string, tempfilepath string) error
	InitRemote(a *GitAnnex) error
	CheckPresent(a *GitAnnex, key string) (present bool, err error)
	Remove(a *GitAnnex, key string) error
}

func (p *GitAnnex) mainloop(helper Helper) error {
	if err := p.writeLine("VERSION 1"); err != nil {
		return err
	}
	for {
		line, err := p.readLine()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		arguments := strings.Split(line, " ")
		switch arguments[0] {
		case "EXTENSIONS":
			if err := p.writeLine("EXTENSIONS"); err != nil {
				return err
			}
		case "LISTCONFIGS":
			if len(arguments) != 1 {
				return errors.New("invalid command: LISTCONFIGS with arguments")
			}
			configs, err := helper.ListConfigs()
			if err != nil {
				return err
			}
			for _, config := range configs {
				if len(config.Name) == 0 || strings.ContainsRune(config.Name, ' ') {
					return fmt.Errorf("api error: refusing to return invalid config name %q", config.Name)
				}
				if err := p.writeLine("CONFIG " + config.Name + " " + config.Description); err != nil {
					return err
				}
			}
			if err := p.writeLine("CONFIGEND"); err != nil {
				return err
			}
		case "INITREMOTE":
			if len(arguments) != 1 {
				return errors.New("invalid command: INITREMOTE with arguments")
			}
			pErr := helper.InitRemote(p)
			var reply string
			if pErr != nil {
				reply = "INITREMOTE-FAILURE nightmarket: " + pErr.Error()
			} else {
				reply = "INITREMOTE-SUCCESS"
			}
			if err := p.writeLine(reply); err != nil {
				return multierror.Append(err, pErr)
			}
		case "PREPARE":
			if len(arguments) != 1 {
				return errors.New("invalid command: PREPARE with arguments")
			}
			pErr := helper.Prepare(p)
			var reply string
			if pErr != nil {
				reply = "PREPARE-FAILURE nightmarket: " + pErr.Error()
			} else {
				reply = "PREPARE-SUCCESS"
			}
			if err := p.writeLine(reply); err != nil {
				return multierror.Append(err, pErr)
			}
		case "TRANSFER":
			if len(arguments) < 4 {
				return fmt.Errorf("invalid transfer command: %q", line)
			}
			var transfer func(*GitAnnex, string, string) error
			switch arguments[1] {
			case "STORE":
				transfer = helper.TransferStore
			case "RETRIEVE":
				transfer = helper.TransferRetrieve
			default:
				return fmt.Errorf("unrecognized transfer command: %q", line)
			}
			key := arguments[2]
			filename := strings.Join(arguments[3:], "")
			pErr := transfer(p, key, filename)
			var reply string
			if pErr != nil {
				reply = "TRANSFER-FAILURE " + arguments[1] + " " + key + " nightmarket: " + pErr.Error()
			} else {
				reply = "TRANSFER-SUCCESS " + arguments[1] + " " + key
			}
			if err := p.writeLine(reply); err != nil {
				return multierror.Append(err, pErr)
			}
		case "CHECKPRESENT":
			if len(arguments) != 2 {
				return fmt.Errorf("invalid checkpresent command: %q", line)
			}
			key := arguments[1]
			found, pErr := helper.CheckPresent(p, key)
			var reply string
			if pErr != nil {
				reply = "CHECKPRESENT-UNKNOWN " + key + " nightmarket: " + pErr.Error()
			} else if found {
				reply = "CHECKPRESENT-SUCCESS " + key
			} else {
				reply = "CHECKPRESENT-FAILURE " + key
			}
			if err := p.writeLine(reply); err != nil {
				return multierror.Append(err, pErr)
			}
		case "REMOVE":
			if len(arguments) != 2 {
				return fmt.Errorf("invalid remove command: %q", line)
			}
			key := arguments[1]
			var reply string
			pErr := helper.Remove(p, key)
			if pErr != nil {
				reply = "REMOVE-FAILURE " + key + " nightmarket: " + pErr.Error()
			} else {
				reply = "REMOVE-SUCCESS " + key
			}
			if err := p.writeLine(reply); err != nil {
				return multierror.Append(err, pErr)
			}
		default:
			if err := p.writeLine("UNSUPPORTED-REQUEST"); err != nil {
				return err
			}
		}
	}
}

func Mainloop(helper Helper) {
	if len(os.Args) != 1 {
		_, _ = fmt.Fprintf(os.Stderr, "%s expected zero arguments\n", os.Args[0])
		os.Exit(1)
	}
	p := &GitAnnex{
		readLine: util.ReadLines(os.Stdin),
		output:   os.Stdout,
	}
	err := p.mainloop(helper)
	if err != nil {
		err2 := p.writeLine("ERROR nightmarket: " + err.Error())
		err = multierror.Append(err, err2)
		_, _ = fmt.Fprintf(os.Stderr, "%s loop error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}
