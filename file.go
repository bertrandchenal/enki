package enki

import (
	"bytes"
	"os"
	"io"
	"crypto/md5"
	// "encoding/hex"
)

const (
	StrongHashSize = md5.Size
	BlockSize = 1024 * 64
	M         = 1 << 16
)

type Block []byte
type StrongHash [StrongHashSize]byte
type WeakHash uint32
type Instruction struct {
	mode string
	hash []byte
	data []byte
}
type Signature struct {
	Instructions []Instruction
}
type File struct {
	Path string
}


func (self *File) GetChecksum() ([]byte, error) {
	fd, err := os.Open(self.Path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	checksum := md5.New()
	_, err = io.Copy(checksum, fd)
	if err != nil {
		return nil, err
	}

	return checksum.Sum(nil), nil
}


func (self *File) Distill(store Store) (sgn *Signature, err error){
	var aweak, bweak, weak, oldWeak WeakHash
	var readSize, blockOffset int64
	var isRolling, matchFound bool
	var data []byte
	var partialReadSize int
	oldBlock := Block{}
	newBlock  := Block{}
	fullBlock := Block(make([]byte, BlockSize))

	// Open file and get his size
	fd, err := os.Open(self.Path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	info, err := fd.Stat()
	check(err)
	fileSize := info.Size()

	// File too small to run deduplication
	if fileSize < BlockSize {
		data = make([]byte, BlockSize)
		partialReadSize, err = fd.Read(data)
		check(err)
		sgn.AddData(data[:partialReadSize])
		return sgn, nil
	}

	// Prepare loop
	data = make([]byte, BlockSize)
	partialReadSize, err = fd.Read(data)
	check(err)
	readSize = BlockSize
	oldBlock = Block(data[:])

	data = make([]byte, BlockSize)
	partialReadSize, err = fd.Read(data)
	if err != io.EOF {
		check(err)
	}
	readSize += int64(partialReadSize)
	newBlock = Block(data[:partialReadSize])
	isRolling = false

	for {


		if matchFound || blockOffset == BlockSize {
			if matchFound {
				matchFound = false
			} else {
				// Store old block
				strong := GetStrongHash(oldBlock)
				oldWeak, _, _ = GetWeakHash(oldBlock)
				println("add old", oldWeak)
				store.AddBlock(oldWeak, strong, oldBlock)
				sgn.AddHash(oldWeak, strong)
				blockOffset = 0
			}

			// Read data
			data = make([]byte, BlockSize)
			partialReadSize, err = fd.Read(data)
			if err == io.EOF {
			} else {
				check(err)
			}
			readSize += int64(partialReadSize)
			oldBlock = newBlock
			newBlock = Block(data[:partialReadSize])
			println("readSize", readSize, fileSize, blockOffset, partialReadSize)

		}

		if readSize == fileSize && blockOffset >= int64(partialReadSize) {
			return sgn, nil
		}


		if !isRolling {
			fullBlock = concat(
				oldBlock[blockOffset:],
				newBlock[:blockOffset],
			)
			weak, aweak, bweak = GetWeakHash(fullBlock)
			isRolling = true
		} else {
			pushHash := WeakHash(newBlock[blockOffset])
			popHash := WeakHash(oldBlock[blockOffset])
			aweak = (aweak - popHash + pushHash) % M
			bweak = (bweak - (WeakHash(BlockSize) * popHash) + aweak) % M
			weak = aweak + (M * bweak)
			blockOffset += 1
		}

		if store.SearchWeak(weak) {
			fullBlock = concat(
				oldBlock[blockOffset:],
				newBlock[:blockOffset],
			)
			strong := GetStrongHash(fullBlock[:])
			blockFound := store.SearchStrong(strong)
			if blockFound {
				println("BLOCK FOUND", weak)
				isRolling = false
				matchFound = true
				continue
			}
		}
	}

}


// Returns a strong hash for a given block of data
func GetStrongHash(v Block) *StrongHash {
	res := StrongHash(md5.Sum(v))
	return &res
}

// Returns a weak hash for a given block of data.
func GetWeakHash(v Block) (WeakHash, WeakHash, WeakHash) {
	var a, b WeakHash
	for i := range v {
		a += WeakHash(v[i])
		b += WeakHash(len(v) - i) * WeakHash(v[i])
	}
	a = a % M
	b = b % M
	weak := a + (M * b)
	return weak, a, b
}

// Returns the smaller of a or b.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Concat variadic arguments
func concat(s...[]byte) []byte {
	return bytes.Join(s, []byte(""))
}


func (self *Signature) AddData(data []byte) {
	// println("SGN:ADDDATA")

}

func (self *Signature) AddHash(weak WeakHash, strong *StrongHash) {
	// println("SGN:ADDHASH")

}
