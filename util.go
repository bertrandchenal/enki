package enki

import (
	"crypto/md5"
	"io"
	"os"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func GetChecksum(path string) ([]byte, error) {
	fd, err := os.Open(path)
	defer fd.Close()
	if err != nil {
		return nil, err
	}

	checksum := md5.New()
	_, err = io.Copy(checksum, fd)
	if err != nil {
		return nil, err
	}
	return checksum.Sum(nil), nil
}
