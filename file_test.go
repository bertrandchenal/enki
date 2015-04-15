package enki

import (
	"fmt"
	"io"
	"os"
	"testing"
 	"encoding/hex"
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


func createFile(nbCopy int, name string, shift bool) string {
	name = "test-enki-" + name
	println("File created:", name)
	fd, err := os.Create(name)
	check(err)
	src, err := os.Open("32.jpg")
	check(err)

	// Shift content by inserting 1 byte at the start of the file
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

	shiftBigFile := createFile(50, "shift-big.data", true)
	f = File{Path: shiftBigFile}
	f.Distill(&store)

	// TODO test with non-repeating pattern content

}

