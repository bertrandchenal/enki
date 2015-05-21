package enki

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path"
	"testing"
 	"encoding/hex"
)

type TestFile struct {
	nbCopy int
	name string
	shifted bool
	random bool
}

var memoryBackend, boltBackend Backend
var testFiles []TestFile
var memoryBlob *Blob
var boltBlob *Blob
const test_data = "test-data"

func TestChecksum(t *testing.T) {
	expect := "c709067ec00d61db0c75d35ace87e21d"

	result, err := GetChecksum("32.jpg")
	if err != nil {
		t.Errorf(err.Error())
	}

	if fmt.Sprintf("%x", result) != expect {
		t.Errorf("Checksum mismatch", string(result), expect)
	}
}


func TestWeakHash(t *testing.T) {
	var weak, aweak, bweak WeakHash
	var i uint32
	content := []byte{8, 0, 1, 2, 5, 6, 7, 9, 3, 4,}
	winSize := uint32(len(content))

	// Init values
	expectWeak, expectA, expectB := GetWeakHash(Block(content))
	weak, aweak, bweak = expectWeak, expectA, expectB

	// Roll, with noise in the middle
	twice := concat(content, content[0:3], content)
	max_len := uint32(len(twice))
	for i = 0; i < max_len - winSize; i++ {
		pushHash := WeakHash(twice[i + winSize])
		popHash := WeakHash(twice[i])
		aweak = (aweak - popHash + pushHash) % M
		bweak = (bweak - (WeakHash(winSize) * popHash) + aweak) % M
		weak = aweak + (M * bweak)
	}
	if expectWeak != weak {
		panic("Failed test on weak hash")
	}
	if expectA != aweak {
		panic("Failed test on 'a' value")
	}
	if expectB != bweak {
		panic("Failed test on 'b' value")
	}

	// Roll, starting with a zeroed prefix
	weak, aweak, bweak = 0, 0, 0
	zeroes := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	twice = concat(zeroes, content)
	max_len = uint32(len(twice))
	for i = 0; i < max_len - winSize; i++ {
		pushHash := WeakHash(twice[i + winSize])
		popHash := WeakHash(twice[i])
		aweak = (aweak - popHash + pushHash) % M
		bweak = (bweak - (WeakHash(winSize) * popHash) + aweak) % M
		weak = aweak + (M * bweak)
	}
	if expectWeak != weak {
		panic("Failed test on weak hash")
	}
	if expectA != aweak {
		panic("Failed test on 'a' value")
	}
	if expectB != bweak {
		panic("Failed test on 'b' value")
	}
}

func TestConcat(t *testing.T) {
	half := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	full := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	// concat two half
	result := hex.EncodeToString(concat(half[:], half[:]))
	expected := hex.EncodeToString(full[:])
	if result != expected {
		println(result, expected)
		panic("Concat test failed")
	}

	// concat two half with range
	result = hex.EncodeToString(concat(half[:3], half[3:]))
	expected = hex.EncodeToString(half[:])
	if result != expected {
		println(result, expected)
		panic("Concat test failed")
	}

	// Use concat as copy
	result = hex.EncodeToString(concat(half[:]))
	expected = hex.EncodeToString(half[:])
	if result != expected {
		println(result, expected)
		panic("Concat test failed")
	}

}


func initFile(testFile *TestFile) {
	fd, err := os.Create(testFile.name)
	check(err)

	if testFile.random {
		src := rand.Reader
		check(err)
		io.CopyN(fd, src, 1024 * int64(testFile.nbCopy))
		fd.Close()
		return
	}

	src, err := os.Open("32.jpg")
	check(err)

	// Shift content by inserting 1 byte at the start of the file
	if testFile.shifted {
		head := [1]byte{}
		src.Read(head[:])
		fd.Write(head[:])
	}

	for i := 0; i < testFile.nbCopy; i++ {
		src.Seek(0, 0)
		io.Copy(fd, src)
		// io.CopyN(fd, src, 1024)
	}
	fd.Close()
	src.Close()
}

func checkSignature(backend Backend, blob *Blob) {
	for _, tf := range testFiles {
		fd, err := os.Open(tf.name)
		check(err)
		sgn, err := blob.GetSignature(fd)
		check(err)
		fd.Close()

		expected, err := GetChecksum(tf.name)
		check(err)

		extracted_path := tf.name + ".extracted"
		fd, err = os.Create(extracted_path)
		check(err)
		sgn.Extract(backend, fd)
		fd.Close()

		checksum, err := GetChecksum(extracted_path)
		check(err)
		if (bytes.Compare(expected, checksum) != 0) {
			panic("Wrong checksum!")
		}
	}
}

func TestMemorySignature(t *testing.T) {
	checkSignature(memoryBackend, memoryBlob)
}

func TestBoltSignature(t *testing.T) {
	checkSignature(boltBackend, boltBlob)
}

func BenchmarkMemorySignature(b *testing.B) {
	checkSignature(memoryBackend, memoryBlob)
}

func BenchmarkBoltSignature(b *testing.B) {
	checkSignature(boltBackend, boltBlob)
}

func TestMain(m *testing.M) {
	memoryBackend = NewMemoryBackend().(Backend)
	memoryBlob = &Blob{memoryBackend}
	boltBackend = NewBoltBackend(test_data)
	boltBlob = &Blob{boltBackend}


	check(os.MkdirAll(test_data, 0750))
	testFiles = []TestFile{
		{1, path.Join(test_data, "small.data"), false, false},
		{10, path.Join(test_data, "larger.data"), false, false},
		{200, path.Join(test_data, "big.data"), false, false},
		{200, path.Join(test_data, "big-shifted.data"), true, false},
		{2000, path.Join(test_data, "random.data"), false, true},
	}
	for _, tf := range testFiles {
		initFile(&tf)
	}
	res := m.Run()
	boltBackend.(*BoltBackend).Close()
	os.Exit(res)
}
