package annexremote

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/celskeggs/nightmarket/lib/util"
	"github.com/hashicorp/go-multierror"
)

const TraceIO = true

type GitAnnex struct {
	readLine func() (string, error)
	output   io.StringWriter
	outLock  sync.Mutex
}

type Responder struct {
	a        *GitAnnex
	job      int // or 0 if no job
	receiver chan string
}

func (r *Responder) provideLine(args string) error {
	select {
	case r.receiver <- args:
		return nil
	default:
		return fmt.Errorf("received nested response data for job worker %d when none was expected", r.job)
	}
}

func (r *Responder) terminate() {
	close(r.receiver)
}

func (r *Responder) readLine() (string, error) {
	line, ok := <-r.receiver
	if !ok {
		return "", io.EOF
	}
	return line, nil
}

func (r *Responder) writeLine(line string) error {
	if r.job < 0 {
		panic("invalid job")
	}
	if r.job > 0 {
		line = fmt.Sprint("J ", r.job, " ", line)
	}
	return r.a.writePlainLine(line)
}

func (a *GitAnnex) writePlainLine(line string) error {
	if strings.ContainsRune(line, '\n') {
		return fmt.Errorf("api error: refusing to write line containing interlinear newline: %q", line)
	}
	a.outLock.Lock()
	defer a.outLock.Unlock()
	_, err := a.output.WriteString(line + "\n")
	if TraceIO {
		_, _ = fmt.Fprintf(os.Stderr, "TO ANNEX:  %q\n", line)
	}
	return err
}

func (r *Responder) Progress(bytes uint64) error {
	return r.writeLine(fmt.Sprint("PROGRESS ", bytes))
}

func (r *Responder) readValue() (string, error) {
	line, err := r.readLine()
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

func (r *Responder) DirHash(key string) (string, error) {
	if err := r.writeLine("DIRHASH " + key); err != nil {
		return "", err
	}
	return r.readValue()
}

func (r *Responder) DirHashLower(key string) (string, error) {
	if err := r.writeLine("DIRHASH-LOWER " + key); err != nil {
		return "", err
	}
	return r.readValue()
}

func (r *Responder) SetConfig(setting, value string) error {
	if len(setting) == 0 || strings.ContainsRune(setting, ' ') {
		return fmt.Errorf("api error: refusing to transmit invalid config name %q", setting)
	}
	return r.writeLine("SETCONFIG " + setting + " " + value)
}

func (r *Responder) GetConfig(setting string) (string, error) {
	if err := r.writeLine("GETCONFIG " + setting); err != nil {
		return "", err
	}
	return r.readValue()
}

func (r *Responder) SetCreds(setting, user, password string) error {
	if len(setting) == 0 || strings.ContainsRune(setting, ' ') {
		return fmt.Errorf("api error: refusing to transmit invalid creds name %q", setting)
	}
	if len(user) == 0 || strings.ContainsRune(user, ' ') {
		return fmt.Errorf("api error: refusing to transmit invalid creds username %q", user)
	}
	return r.writeLine("SETCREDS " + setting + " " + user + " " + password)
}

func (r *Responder) GetCreds(setting string) (string, string, error) {
	if err := r.writeLine("GETCREDS " + setting); err != nil {
		return "", "", err
	}
	result, err := r.readLine()
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

func (r *Responder) GetUUID() (string, error) {
	if err := r.writeLine("GETUUID"); err != nil {
		return "", err
	}
	return r.readValue()
}

func (r *Responder) GetGitDir() (string, error) {
	if err := r.writeLine("GETGITDIR"); err != nil {
		return "", err
	}
	return r.readValue()
}

func (r *Responder) SetState(key, value string) error {
	if len(key) == 0 || strings.ContainsRune(key, ' ') {
		return fmt.Errorf("api error: refusing to transmit invalid state name %q", key)
	}
	return r.writeLine("SETSTATE " + key + " " + value)
}

func (r *Responder) GetState(key string) (string, error) {
	if err := r.writeLine("GETSTATE " + key); err != nil {
		return "", err
	}
	return r.readValue()
}

func (r *Responder) Debug(message string) error {
	return r.writeLine("DEBUG " + message)
}

type Config struct {
	Name        string
	Description string
}

type Helper interface {
	NegotiateAsync() bool
	ListConfigs() ([]Config, error)
	Prepare(a *Responder) error
	TransferStore(a *Responder, key string, tempfilepath string) error
	TransferRetrieve(a *Responder, key string, tempfilepath string) error
	InitRemote(a *Responder) error
	CheckPresent(a *Responder, key string) (present bool, err error)
	Remove(a *Responder, key string) error
}

func stringsContain(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func (a *GitAnnex) command(resp *Responder, helper Helper, line string) error {
	arguments := strings.Split(line, " ")
	switch arguments[0] {
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
			if err := resp.writeLine("CONFIG " + config.Name + " " + config.Description); err != nil {
				return err
			}
		}
		if err := resp.writeLine("CONFIGEND"); err != nil {
			return err
		}
	case "INITREMOTE":
		if len(arguments) != 1 {
			return errors.New("invalid command: INITREMOTE with arguments")
		}
		pErr := helper.InitRemote(resp)
		var reply string
		if pErr != nil {
			reply = "INITREMOTE-FAILURE nightmarket: " + pErr.Error()
		} else {
			reply = "INITREMOTE-SUCCESS"
		}
		if err := resp.writeLine(reply); err != nil {
			return multierror.Append(err, pErr)
		}
	case "PREPARE":
		if len(arguments) != 1 {
			return errors.New("invalid command: PREPARE with arguments")
		}
		pErr := helper.Prepare(resp)
		var reply string
		if pErr != nil {
			reply = "PREPARE-FAILURE nightmarket: " + pErr.Error()
		} else {
			reply = "PREPARE-SUCCESS"
		}
		if err := resp.writeLine(reply); err != nil {
			return multierror.Append(err, pErr)
		}
	case "TRANSFER":
		if len(arguments) < 4 {
			return fmt.Errorf("invalid transfer command: %q", line)
		}
		var transfer func(*Responder, string, string) error
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
		pErr := transfer(resp, key, filename)
		var reply string
		if pErr != nil {
			reply = "TRANSFER-FAILURE " + arguments[1] + " " + key + " nightmarket: " + pErr.Error()
		} else {
			reply = "TRANSFER-SUCCESS " + arguments[1] + " " + key
		}
		if err := resp.writeLine(reply); err != nil {
			return multierror.Append(err, pErr)
		}
	case "CHECKPRESENT":
		if len(arguments) != 2 {
			return fmt.Errorf("invalid checkpresent command: %q", line)
		}
		key := arguments[1]
		found, pErr := helper.CheckPresent(resp, key)
		var reply string
		if pErr != nil {
			reply = "CHECKPRESENT-UNKNOWN " + key + " nightmarket: " + pErr.Error()
		} else if found {
			reply = "CHECKPRESENT-SUCCESS " + key
		} else {
			reply = "CHECKPRESENT-FAILURE " + key
		}
		if err := resp.writeLine(reply); err != nil {
			return multierror.Append(err, pErr)
		}
	case "REMOVE":
		if len(arguments) != 2 {
			return fmt.Errorf("invalid remove command: %q", line)
		}
		key := arguments[1]
		var reply string
		pErr := helper.Remove(resp, key)
		if pErr != nil {
			reply = "REMOVE-FAILURE " + key + " nightmarket: " + pErr.Error()
		} else {
			reply = "REMOVE-SUCCESS " + key
		}
		if err := resp.writeLine(reply); err != nil {
			return multierror.Append(err, pErr)
		}
	default:
		if err := resp.writeLine("UNSUPPORTED-REQUEST"); err != nil {
			return err
		}
	}
	return nil
}

func parseJobId(arguments string, isAsync bool) (string, int, error) {
	if !isAsync {
		if strings.HasPrefix(arguments, "J ") {
			return "", 0, fmt.Errorf("not in ASYNC mode; should not have receiver J prefix in %q", arguments)
		}
		// job ID 0
		return arguments, 0, nil
	} else {
		parts := strings.SplitN(arguments, " ", 3)
		if parts[0] != "J" {
			return "", 0, fmt.Errorf("in ASYNC mode; should have received J prefix in %q", arguments)
		}
		if len(parts) != 3 {
			return "", 0, fmt.Errorf("in ASYNC mode; needed arguments after prefix in %q", arguments)
		}
		jobNum, err := strconv.ParseUint(parts[1], 10, 31)
		if err != nil {
			return "", 0, err
		}
		if jobNum < 1 {
			return "", 0, fmt.Errorf("invalid ASYNC job number: %d", jobNum)
		}
		return parts[2], int(jobNum), nil
	}
}

func (a *GitAnnex) startResponder(helper Helper, jobNum int, wg *sync.WaitGroup, errCh chan<- error) *Responder {
	resp := &Responder{
		a:        a,
		job:      jobNum,
		receiver: make(chan string, 1),
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			args, err := resp.readLine()
			if err != nil {
				if err != io.EOF {
					errCh <- err
				}
				break
			}
			if err := a.command(resp, helper, args); err != nil {
				errCh <- err
				break
			}
		}
	}()
	return resp
}

func (a *GitAnnex) mainloop(helper Helper) error {
	if err := a.writePlainLine("VERSION 1"); err != nil {
		return err
	}
	isAsync := false
	lines := make(chan string)
	var wg sync.WaitGroup
	var readErr chan error
	go func() {
		// note: if an error occurs in a job, this goroutine will not terminate normally, because it contains no
		// mechanism to break out of the read. this is fine, because in such a case, the entire program will terminate
		// promptly.
		defer close(lines)
		for {
			line, err := a.readLine()
			if err != nil {
				if err != io.EOF {
					readErr <- err
				}
				break
			}
			if TraceIO {
				_, _ = fmt.Fprintf(os.Stderr, "TO HELPER: %q\n", line)
			}
			lines <- line
		}
	}()
	errCh := make(chan error)
	responders := map[int]*Responder{}
	var collectedErrors error
loop:
	for collectedErrors == nil {
		select {
		case e := <-errCh:
			if e == nil {
				panic("should always be an error")
			}
			collectedErrors = e
		case e := <-readErr:
			if e == nil {
				panic("should always be an error")
			}
			collectedErrors = e
		case line, ok := <-lines:
			if !ok {
				break loop
			}
			if strings.HasPrefix(line, "EXTENSIONS") {
				arguments := strings.Split(line, " ")
				extensions := "EXTENSIONS"
				if stringsContain(arguments[1:], "ASYNC") && helper.NegotiateAsync() {
					isAsync = true
					extensions += " ASYNC"
				}
				collectedErrors = a.writePlainLine(extensions)
			} else {
				cmdArgs, jobNum, err := parseJobId(line, isAsync)
				if err != nil {
					collectedErrors = err
					break
				}
				resp, found := responders[jobNum]
				if !found {
					resp = a.startResponder(helper, jobNum, &wg, errCh)
					responders[jobNum] = resp
				}
				collectedErrors = resp.provideLine(cmdArgs)
			}
		}
	}
	// once we hit an error -- or EOF -- terminate all jobs
	for _, resp := range responders {
		resp.terminate()
	}
	// receive all errors until all jobs terminate
	go func() {
		wg.Wait()
		close(errCh)
	}()
	for e := range errCh {
		collectedErrors = multierror.Append(collectedErrors, e)
	}
	select {
	case e := <-readErr:
		collectedErrors = multierror.Append(collectedErrors, e)
	default:
	}
	return collectedErrors
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
		err2 := p.writePlainLine("ERROR nightmarket: " + err.Error())
		err = multierror.Append(err, err2)
		_, _ = fmt.Fprintf(os.Stderr, "%s loop error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}
