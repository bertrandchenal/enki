package enki

import (
	"fmt"
	"testing"
	"io"
	"os"
)

func TestChecksum(t *testing.T) {
	expect := "c709067ec00d61db0c75d35ace87e21d"
	f := File{Path: "32.jpg"}
	result, err := f.GetChecksum()

	if err != nil {
		t.Errorf(err.Error())
	}

	if fmt.Sprintf("%x", result) != expect {
		t.Errorf("Checksum mismatch", string(result), expect)
	}

}

func createFile(nbCopy int, name string) string {
	name = "test-enki-" + name
	println("File created:", name)
	fd, err := os.Create(name)
	check(err)
	src, err := os.Open("32.jpg")
	check(err)
	for i := 0; i < nbCopy; i++ {
		io.Copy(fd, src)
		src.Seek(0, 0)
	}
	fd.Close()
	src.Close()
	return name
}

func TestDistill(t *testing.T) {
	smallFile := createFile(1, "small.data")

	f := File{Path: smallFile}
	store := DummyStore{}
	store.WeakMap = make(map[WeakHash]bool)
	store.BlockMap = make(map[StrongHash]Block)
	f.Distill(&store)

	largerFile := createFile(10, "larger.data")
	f = File{Path: largerFile}
	f.Distill(&store)

	bigFile := createFile(50, "big.data")
	f = File{Path: bigFile}
	f.Distill(&store)
	f.Distill(&store)
}

