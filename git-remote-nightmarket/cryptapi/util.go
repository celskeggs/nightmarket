package cryptapi

import (
	"errors"
	"io"
	"io/ioutil"
	"os"

	"github.com/hashicorp/go-multierror"
)

type bufferedFile struct {
	f       *os.File
	deleted bool
}

func (b *bufferedFile) Seek(offset int64, whence int) (int64, error) {
	if b.deleted {
		return 0, errors.New("buffered file already deleted")
	}
	return b.f.Seek(offset, whence)
}

func (b *bufferedFile) Read(p []byte) (n int, err error) {
	if b.deleted {
		return 0, errors.New("buffered file already deleted")
	}
	return b.f.Read(p)
}

func (b *bufferedFile) Close() error {
	err := b.f.Close()
	if !b.deleted {
		b.deleted = true
		err2 := os.Remove(b.f.Name())
		if err2 != nil {
			err = multierror.Append(err, err2)
		}
	}
	return err
}

func BufferInFile(r io.Reader) (rc io.ReadSeekCloser, e error) {
	f, err := ioutil.TempFile("", "file-buffer")
	if err != nil {
		return nil, err
	}
	b := &bufferedFile{f: f}
	defer func() {
		// if we fail, make sure to close the buffered file, which will also delete it from the filesystem.
		if rc == nil {
			e = multierror.Append(e, b.Close())
		}
	}()
	if _, err = io.Copy(f, r); err != nil {
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return b, nil
}

type CombinedReadCloser struct {
	io.Reader
	io.Closer
}

var _ io.ReadCloser = CombinedReadCloser{}
