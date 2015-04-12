package enki

import (
	"bytes"
	"os"
	"io"
	"crypto/md5"
	"encoding/hex"
	// "reflect"
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
	var readSize int64
	var isRolling bool
	var data [BlockSize]byte
	var matchOffset, partialReadSize int
	blockOffset := BlockSize - 1 // Will bootstrap read
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
		partialReadSize, err = fd.Read(data[:])
		check(err)
		sgn.AddData(data[:partialReadSize])
		return sgn, nil
	}


	// Loop on file content: at any time in the loop newBlock and
	// oldBlock are the two last block of data read from the file. The
	// rolling windows is moving astride them and when it is on top of
	// newBlock (no more on oldBlock) the part of the oldBlock that is
	// not in the store is added. If a part of the oldBlock is in the
	// store (which means a prefix of oldBlock has been match to
	// existing data in the store) then we put the suffix in the
	// signature.
	for {

		if readSize > fileSize {
			panic("Out of bound read")
		}

		// We read a new block if we reach the end of the current
		// block or if there is a match
		if blockOffset >= BlockSize - 1 || matchOffset > 0 {
			if matchOffset == 0 && len(oldBlock) > 0  {
				// Put unprocessed oldBlock in store
				strong := GetStrongHash(oldBlock)
				store.AddBlock(oldWeak, strong, oldBlock)
				sgn.AddHash(oldWeak, strong)
			}
			// Read new block
			partialReadSize, err = fd.Read(data[:])
			check(err)
			readSize += int64(partialReadSize)

			if matchOffset > 0 {
				// Jump over matched data
				isRolling = false
				blockOffset = BlockSize - matchOffset
				matchOffset = 0
			} else {
				blockOffset = 0
			}
			// Update old & new
			oldWeak = weak
			oldBlock = newBlock
			newBlock = Block(data[:partialReadSize])
		}

		// Handle end of file
		if readSize == fileSize && blockOffset == partialReadSize {
			// Store old block
			strong := GetStrongHash(oldBlock)
			store.AddBlock(oldWeak, strong, oldBlock)
			sgn.AddHash(oldWeak, strong)

			// Add end of file
			if len(newBlock) > 0 {
				sgn.AddData(newBlock[:])
			}
			if len(newBlock) >= BlockSize {
				panic("Unexpected size of last read block")
			}
			return sgn, nil
		}

		// Update weak hash
		if !isRolling {
			// Init weak hash
			weak, aweak, bweak = GetWeakHash(newBlock)
			isRolling = true
			// We have consumed the block, fast forward to next
			blockOffset = BlockSize
		} else {
			// Roll
			pushHash := WeakHash(newBlock[blockOffset])
			popHash := WeakHash(oldBlock[blockOffset])
			aweak = (aweak - popHash + pushHash) % M
			bweak = (bweak - (WeakHash(BlockSize+1) * popHash) + aweak) % M
			weak = aweak + (M * bweak)
		}

		// handle weak hash match
		if store.SearchWeak(weak) {
			copy(concat(
				oldBlock[BlockSize - blockOffset:],
				newBlock[0:blockOffset],
			), fullBlock[:])

			strong := GetStrongHash(fullBlock[:])
			blockFound := store.SearchStrong(strong)
			println(blockFound, " -- ",hex.Dump(strong[:]))
			if blockFound {
				// add partial data
				sgn.AddData(oldBlock[0:blockOffset])
				// add matching block
				sgn.AddHash(weak, strong)
				matchOffset = blockOffset
			}
		}
		blockOffset += 1
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
		b += WeakHash(len(v) - i + 1) * WeakHash(v[i])
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
