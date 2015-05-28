package enki

import (
	"bytes"
	"io"
	"crypto/md5"
)

const (
	StrongHashSize = md5.Size
	BlockSize = 1024 * 64
	M         = 1 << 16
	DATA_SGM = iota
	HASH_SGM = iota
)

type Block []byte
type StrongHash [StrongHashSize]byte
type WeakHash uint32
type Blob struct {
	backend Backend
}



func (self *Blob) GetSignature(fd io.Reader) (sgn *Signature, err error) {
	var aweak, bweak, weak, oldWeak WeakHash
	var readSize, partialReadSize, blockOffset, lastMatch int64
	var isRolling, matchFound, eofReached bool
	var data []byte
	oldBlock := Block{}
	newBlock := Block{}
	fullBlock := Block(make([]byte, BlockSize))
	sgn = &Signature{}

	// Read first block
	data = make([]byte, BlockSize)
	prs, err := io.ReadFull(fd, data)
	partialReadSize = int64(prs)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			sgn.AddData(data[:partialReadSize])
			return sgn, nil
		} else if err == io.EOF {
			return nil, nil
		} else {
			panic(err)
		}
	}
	readSize = BlockSize
	oldBlock = Block(data[:])

	// Read second block
	data = make([]byte, BlockSize)
	prs, err = io.ReadFull(fd, data)
	partialReadSize = int64(prs)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			strong := GetStrongHash(oldBlock)
			weak, _, _ = GetWeakHash(oldBlock)
			self.backend.AddBlock(weak, strong, oldBlock)
			sgn.AddHash(oldWeak, strong)
			sgn.AddData(data[:partialReadSize])
			return sgn, nil
		} else if err == io.EOF {
			sgn.AddData(oldBlock)
			return sgn, nil
		} else {
			panic(err)
		}
	}
	readSize += BlockSize
	newBlock = Block(data[:])
	isRolling = false
	for {
		// Read new block when end of block is reached or if a match
		// was found
		if matchFound || blockOffset == partialReadSize {
			if matchFound {
				isRolling = false
				matchFound = false
				lastMatch = blockOffset
			} else {
				// Store old block
				if lastMatch > 0 {
					sgn.AddData(oldBlock[lastMatch:])
				} else {
					strong := GetStrongHash(oldBlock)
					oldWeak, _, _ = GetWeakHash(oldBlock)
					self.backend.AddBlock(oldWeak, strong, oldBlock)
					sgn.AddHash(oldWeak, strong)
				}
				blockOffset = 0
				lastMatch = 0
			}

			// Last read was too short
			if partialReadSize < BlockSize {
				sgn.AddData(newBlock[blockOffset:partialReadSize])
				return sgn, nil
			}

			// Read data
			data = make([]byte, BlockSize)
			prs, err := io.ReadFull(fd, data)
			partialReadSize = int64(prs)
			if err != io.EOF && err != io.ErrUnexpectedEOF{
				check(err)
			} else {
				eofReached = true
			}
			readSize += partialReadSize
			oldBlock = newBlock
			newBlock = Block(data[:partialReadSize])
		}
		if eofReached && blockOffset >= partialReadSize {
			if lastMatch > 0 {
				sgn.AddData(oldBlock[lastMatch:])
			} else {
				strong := GetStrongHash(oldBlock)
				oldWeak, _, _ = GetWeakHash(oldBlock)
				self.backend.AddBlock(oldWeak, strong, oldBlock)
				sgn.AddHash(oldWeak, strong)
			}
			sgn.AddData(newBlock[:partialReadSize])
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

		if self.backend.SearchWeak(weak) {
			fullBlock = concat(
				oldBlock[blockOffset:],
				newBlock[:blockOffset],
			)
			strong := GetStrongHash(fullBlock[:])
			if self.backend.GetStrong(strong) != nil {
				matchFound = true
				sgn.AddData(oldBlock[lastMatch:blockOffset])
				sgn.AddHash(weak, strong)
			}
		}
	}

}

func (self *Blob) Restore(checksum []byte, w io.Writer) (nb_bytes int){
	sgn := self.backend.GetSignature(checksum)
	if sgn == nil {
		return 0
	}
	sgn.Extract(self.backend, w)
	return 0 // USEFULL ?
}

func (self *Blob) Snapshot(checksum []byte, fd io.Reader) {
	sgn, err := self.GetSignature(fd)
	check(err)
	self.backend.SetSignature(checksum, sgn)
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
