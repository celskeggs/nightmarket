package nmcmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/celskeggs/nightmarket/lib/cryptapi"
	"github.com/celskeggs/nightmarket/lib/util"
	"github.com/hashicorp/go-multierror"
)

func gitInit(path, initialBranch string) error {
	cmd := exec.Command("git", "init", "-b", initialBranch)
	cmd.Dir = path
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func gitAnnexInit(path string) error {
	cmd := exec.Command("git", "annex", "init")
	cmd.Dir = path
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func gitAnnexSync(path string) error {
	cmd := exec.Command("git", "annex", "sync")
	cmd.Dir = path
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func gitAnnexEnableRemote(path string, remote string) error {
	cmd := exec.Command("git", "annex", "enableremote", "--", remote)
	cmd.Dir = path
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func gitRemoteAdd(path string, remote string, url string) error {
	cmd := exec.Command("git", "remote", "add", "--", remote, url)
	cmd.Dir = path
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func gitGetGlobalConfig(key string) (string, error) {
	cmd := exec.Command("git", "config", "--get", "--", key)
	cmd.Stderr = os.Stderr
	value, err := cmd.Output()
	if err != nil {
		if x, ok := err.(*exec.ExitError); ok && x.ExitCode() == 1 && len(x.Stderr) == 0 {
			return "", fmt.Errorf("git config variable not set: %q", key)
		}
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}

func validateEnvPath() error {
	_, err1 := exec.LookPath("git-remote-nightmarket")
	_, err2 := exec.LookPath("git-annex-remote-nightmarket")
	if err1 != nil || err2 != nil {
		return multierror.Append(err1, err2)
	}
	return nil
}

func validateGitConfig(prompt func(string) (string, error)) error {
	userName, err1 := gitGetGlobalConfig("user.name")
	userEmail, err2 := gitGetGlobalConfig("user.email")
	if err1 != nil || err2 != nil {
		return multierror.Append(err1, err2)
	}
	fmt.Printf("Global identity: name=%q email=%q\n", userName, userEmail)
	reply, err := prompt("Confirm identity (y/n)? ")
	if err != nil {
		return err
	}
	if strings.ToLower(reply) != "y" {
		return errors.New("aborted by user")
	}
	return nil
}

func getConfigDir(create bool) (string, error) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if homedir == "" {
		return "", errors.New("empty $HOME path")
	}
	configDir := path.Join(homedir, ".nightmarket")
	if create {
		err = os.Mkdir(configDir, 0755)
		if err != nil && !errors.Is(err, fs.ErrExist) {
			return "", err
		}
	}
	stat, err := os.Stat(configDir)
	if err != nil {
		return "", err
	}
	if !stat.IsDir() {
		return "", fmt.Errorf("expected %q to be a directory", configDir)
	}
	return configDir, nil
}

func describeExistingConfig(configPath string) (selectable bool, description string) {
	clerk, err := cryptapi.LoadConfig(configPath)
	if err != nil {
		return false, err.Error()
	}
	conf := clerk.Config.SpaceConfig
	return true, fmt.Sprintf("store=%q func=%q device=%q", conf.SpacePrefix, conf.URL, conf.DeviceName)
}

func promptConfig(prompt func(string) (string, error)) (cryptapi.ClerkConfig, error) {
	var config cryptapi.ClerkConfig
	for {
		url, err := prompt("Function DNS Name> ")
		if err != nil {
			return cryptapi.ClerkConfig{}, err
		}
		// make sure this is approximately the right format
		if strings.Contains(url, ".") && !strings.Contains(url, "/") {
			config.SpaceConfig.URL = "https://" + url
			break
		}
		fmt.Printf("Invalid DNS name: %q\n", url)
	}
	for {
		url, err := prompt("Space DNS Name> ")
		if err != nil {
			return cryptapi.ClerkConfig{}, err
		}
		// make sure this is approximately the right format
		if strings.Contains(url, ".") && !strings.Contains(url, "/") {
			config.SpaceConfig.SpacePrefix = "https://" + url + "/"
			break
		}
		fmt.Printf("Invalid DNS name: %q\n", url)
	}
	device, err := prompt("Device Name> ")
	if err != nil {
		return cryptapi.ClerkConfig{}, err
	}
	config.SpaceConfig.DeviceName = device
	token, err := prompt("Device Token> ")
	if err != nil {
		return cryptapi.ClerkConfig{}, err
	}
	config.SpaceConfig.DeviceToken = token
	encryptionKey, err := prompt("Encryption Key> ")
	if err != nil {
		return cryptapi.ClerkConfig{}, err
	}
	config.SecretKey = encryptionKey
	clerk, err := cryptapi.NewClerk(config)
	if err != nil {
		return cryptapi.ClerkConfig{}, err
	}
	factor, err := prompt("Work Factor (10-20 recommended)> ")
	if err != nil {
		return cryptapi.ClerkConfig{}, err
	}
	factorNum, err := strconv.ParseUint(factor, 10, 64)
	if err != nil {
		return cryptapi.ClerkConfig{}, err
	}
	// 22 is the maximum that age will decrypt by default
	if factorNum < 1 || factorNum > 22 {
		return cryptapi.ClerkConfig{}, errors.New("invalid factor")
	}
	config.WorkFactor = int(factorNum)
	if _, err := clerk.ListObjects(); err != nil {
		return cryptapi.ClerkConfig{}, err
	}
	return config, nil
}

func writeJSON(data interface{}, filepath string) (err error) {
	// use 0600 to protect configuration file from other users
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := f.Close(); err2 != nil {
			err = multierror.Append(err, err2)
		}
		if err != nil {
			err = multierror.Append(err, os.Remove(filepath))
		}
	}()
	return json.NewEncoder(f).Encode(data)
}

func promptCreateNewConfig(configDir string, prompt func(string) (string, error)) (string, error) {
	fmt.Printf("To create a new configuration, enter the following information:\n")
	var filepath string
	for {
		filename, err := prompt("Config Name> ")
		if err != nil {
			return "", err
		}
		if filename == "" || strings.Contains(filename, "/") || strings.HasPrefix(filename, ".") {
			fmt.Printf("Invalid filename: %q\n", filename)
			continue
		}
		filepath = path.Join(configDir, filename)
		_, err = os.Stat(filepath)
		if err == nil {
			fmt.Printf("Filename already exists: %q\n", filename)
			continue
		} else if errors.Is(err, fs.ErrNotExist) {
			break
		} else {
			return "", err
		}
	}
	config, err := promptConfig(prompt)
	if err != nil {
		return "", err
	}
	if err = writeJSON(config, filepath); err != nil {
		return "", err
	}
	return filepath, nil
}

type configOption struct {
	Description string
	Filename    string
}

// selectConfiguration returns the path to the appropriate JSON file
func selectConfiguration(configDir string, prompt func(string) (string, error)) (string, error) {
	var options []configOption
	files, err := os.ReadDir(configDir)
	if err != nil {
		return "", err
	}
	const newOption = "New"
	maxName := len(newOption)
	for _, file := range files {
		nameLen := len(file.Name())
		if nameLen > maxName {
			maxName = nameLen
		}
	}
	fmt.Printf("Select one:\n")
	for _, file := range files {
		filename := file.Name()
		selectable, description := describeExistingConfig(path.Join(configDir, filename))
		if selectable {
			options = append(options, configOption{
				Description: description,
				Filename:    filename,
			})
		} else {
			padded := filename + strings.Repeat(" ", maxName-len(filename))
			fmt.Printf(" X. %s -> %s\n", padded, description)
		}
	}
	for i, option := range options {
		padded := option.Filename + strings.Repeat(" ", maxName-len(option.Filename))
		fmt.Printf("%2d. %s -> %s\n", i+1, padded, option.Description)
	}
	padded := newOption + strings.Repeat(" ", maxName-len(newOption))
	fmt.Printf(" C. %s -> Create a new configuration\n", padded)
	for {
		line, err := prompt("Option> ")
		if err != nil {
			return "", err
		}
		if line == "c" || line == "C" {
			return promptCreateNewConfig(configDir, prompt)
		} else {
			index, err := strconv.ParseUint(line, 10, 16)
			intIndex := int(index)
			if err != nil || intIndex < 1 || intIndex > len(options) {
				fmt.Printf("Not a valid option.\n")
				continue
			}
			opt := options[intIndex-1]
			return path.Join(configDir, opt.Filename), nil
		}
	}
}

func initRepo(repoPath string) error {
	if err := validateEnvPath(); err != nil {
		return err
	}
	configDir, err := getConfigDir(true)
	if err != nil {
		return err
	}
	prompt := util.Prompter(os.Stdin, os.Stdout)
	configPath, err := selectConfiguration(configDir, prompt)
	if err != nil {
		return err
	}
	if err := validateGitConfig(prompt); err != nil {
		return err
	}
	if err := os.Mkdir(repoPath, 0755); err != nil {
		return err
	}
	if err := gitInit(repoPath, "latest/main"); err != nil {
		return err
	}
	if err := gitAnnexInit(repoPath); err != nil {
		return err
	}
	if err := gitRemoteAdd(repoPath, "nm", "nightmarket::"+configPath); err != nil {
		return err
	}
	if err := gitAnnexSync(repoPath); err != nil {
		return err
	}
	if err := gitAnnexEnableRemote(repoPath, "nmspec"); err != nil {
		return err
	}
	fmt.Printf("Annex at %q is ready to use!\n", repoPath)
	return nil
}
