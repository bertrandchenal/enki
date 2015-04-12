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


func TestWeakHash(t *testing.T) {
	var weak, a, b WeakHash
	content := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	// Init values
	expectWeak, expectA, expectB := GetWeakHash(Block(content))

	// Roll
	for i := range content {
		a += WeakHash(content[i])
		b += WeakHash(len(content) - i + 1) * WeakHash(content[i])
	}
	a = a % M
	b = b % M
	weak = a + (M * b)

	if expectWeak != weak {
		panic("Failed test on weak hash")
	}

	if expectA != a {
		panic("Failed test on 'a' value")
	}

	if expectB != b {
		panic("Failed test on 'b' value")
	}


}


func createFile(nbCopy int, name string, shift bool) string {
	name = "test-enki-" + name
	println("File created:", name)
	fd, err := os.Create(name)
	check(err)
	src, err := os.Open("32.jpg")
	check(err)

	if shift {
		head := [1]byte{}
		src.Read(head[:])
		fd.Write(head[:])
	}

	for i := 0; i < nbCopy; i++ {
		src.Seek(0, 0)
		io.Copy(fd, src)
	}
	fd.Close()
	src.Close()
	return name
}

func TestDistill(t *testing.T) {
	smallFile := createFile(1, "small.data", false)

	f := File{Path: smallFile}
	store := DummyStore{}
	store.WeakMap = make(map[WeakHash]bool)
	store.BlockMap = make(map[StrongHash]Block)
	f.Distill(&store)

	largerFile := createFile(10, "larger.data", false)
	f = File{Path: largerFile}
	f.Distill(&store)

	bigFile := createFile(50, "big.data", false)
	f = File{Path: bigFile}
	f.Distill(&store)

	shiftBigFile := createFile(50, "shiftbig.data", true)
	f = File{Path: shiftBigFile}
	f.Distill(&store)

}

