package nmcmd

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/celskeggs/nightmarket/lib/annexhelper"
	"github.com/celskeggs/nightmarket/lib/cryptapi"
	"github.com/celskeggs/nightmarket/lib/util"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/crypto/sha3"
)

func repairRepo() error {
	configDir, err := getConfigDir(false)
	if err != nil {
		return err
	}
	prompt := util.Prompter(os.Stdin, os.Stdout)
	configPath, err := selectConfiguration(configDir, prompt)
	if err != nil {
		return err
	}
	clerk, err := cryptapi.LoadConfig(configPath)
	if err != nil {
		return err
	}
	fmt.Println("Preparing to deduplicate unexpected duplicate files in storage bucket...")
	objects, err := clerk.ListObjects()
	if err != nil {
		return err
	}
	duplicates, err := annexhelper.ListDuplicates(objects)
	if err != nil {
		return err
	}
	fmt.Printf("Discovered %d infixes provided by duplicate files.\n", len(duplicates))
	if len(duplicates) == 0 {
		fmt.Println("Nothing to do.")
		return nil
	}
	fmt.Println("Verifying that infix data matches...")
	var deletions []*s3.ObjectIdentifier
	for infix, objectPaths := range duplicates {
		if err := verifyMatching(clerk, infix, objectPaths); err != nil {
			return err
		}
		fmt.Printf("    Passed: %q\n", infix)
		for _, objectPath := range objectPaths[1:] {
			deletions = append(deletions, &s3.ObjectIdentifier{
				Key: aws.String(objectPath),
			})
		}
	}
	fmt.Printf("Security validation passed. Preparing to delete %d objects:\n", len(deletions))
	for _, deletion := range deletions {
		fmt.Printf("    Object: %q\n", *deletion.Key)
	}
	ok, err := prompt("Okay to proceed? (Y/N) ")
	if err != nil {
		return err
	}
	if ok != "Y" && ok != "y" {
		return fmt.Errorf("not okay to proceed")
	}
	api, bucket, err := promptSession(prompt)
	if err != nil {
		return err
	}
	output, err := api.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: bucket,
		Delete: &s3.Delete{
			Objects: deletions,
		},
	})
	if err != nil {
		return err
	}
	if len(output.Errors) > 0 {
		fmt.Printf("Encountered %d errors while deleting (%d successes):\n", len(output.Errors), len(output.Deleted))
		for _, deleteErr := range output.Errors {
			fmt.Printf("    Error: code=%q key=%q description=%q version=%q\n",
				deleteErr.Code, deleteErr.Key, deleteErr.Message, deleteErr.VersionId)
		}
	} else {
		fmt.Printf(
			"Successfully deleted %d objects! Rerun repair and an upload to confirm this was performed correctly.\n",
			len(output.Deleted))
	}
	return nil
}

func promptSession(prompt func(string) (string, error)) (s *s3.S3, bucket *string, err error) {
	region, err := prompt("Enter space region (such as 'nyc3'): ")
	if err != nil {
		return nil, nil, err
	}
	endpoint, err := prompt("Enter space endpoint (such as 'nyc3.digitaloceanspaces.com'): ")
	if err != nil {
		return nil, nil, err
	}
	space, err := prompt("Enter space name (such as 'backup-bucket'): ")
	if err != nil {
		return nil, nil, err
	}
	access, err := prompt("Enter full-privilege access key: ")
	if err != nil {
		return nil, nil, err
	}
	secret, err := prompt("Enter full-privilege secret key: ")
	if err != nil {
		return nil, nil, err
	}
	spacesSession, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(access, secret, ""),
		Endpoint:    aws.String(endpoint),
		Region:      aws.String(region),
	})
	return s3.New(spacesSession), aws.String(space), nil
}

func getHash(clerk *cryptapi.Clerk, objectPath string) (hash string, err error) {
	rc, err := clerk.GetDecryptObjectStream(objectPath)
	if err != nil {
		return "", err
	}
	defer func() {
		if err2 := rc.Close(); err2 != nil {
			err = multierror.Append(err, err2)
		}
	}()
	h := sha3.New512()
	if _, err := io.Copy(h, rc); err != nil {
		return "", err
	}
	hashBytes := h.Sum(nil)
	if len(hashBytes) == 0 {
		panic("invalid length")
	}
	return hex.EncodeToString(hashBytes), nil
}

func verifyMatching(clerk *cryptapi.Clerk, infix string, paths []string) error {
	if len(paths) < 2 {
		panic("should have at least two paths")
	}
	firstHash, err := getHash(clerk, paths[0])
	if err != nil {
		return err
	}
	for _, path := range paths[1:] {
		nextHash, err := getHash(clerk, path)
		if err != nil {
			return err
		}
		if firstHash != nextHash {
			return fmt.Errorf(
				"security alert: duplicate contents of infix %q do not match: %q and %q -- requires further "+
					"investigation before deduplication is possible", infix, firstHash, nextHash)
		}
	}
	return nil
}
