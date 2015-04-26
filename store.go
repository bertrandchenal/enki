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
type Segment struct {
	mode int
	weakHash WeakHash
	strongHash *StrongHash
	data []byte
}
type Signature struct {
	Segments []Segment
}

type Store struct {
	backend Backend
}


func (self *Store) GetChecksum(fd io.Reader) ([]byte, error) {
	checksum := md5.New()
	_, err := io.Copy(checksum, fd)
	if err != nil {
		return nil, err
	}
	return checksum.Sum(nil), nil
}


func (self *Store) GetSignature(fd io.Reader) (sgn *Signature, err error) {
	var aweak, bweak, weak, oldWeak WeakHash
	var readSize, partialReadSize, blockOffset, lastMatch int64
	var isRolling, matchFound, eofReached bool
	var data []byte
	oldBlock := Block{}
	newBlock  := Block{}
	fullBlock := Block(make([]byte, BlockSize))
	sgn = &Signature{}

	// Read first block
	data = make([]byte, BlockSize)
	prs, err := io.ReadFull(fd, data)
	partialReadSize = int64(prs)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			println("Add data")
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
	println("blockOffset", blockOffset, "partialReadSize", partialReadSize)

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
					println("STORE OLD DATA")
					sgn.AddData(oldBlock[lastMatch:])
				} else {
					println("STORE OLD HASH")
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
				println("STOP", blockOffset, partialReadSize)
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
			panic("don't touch me")
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
			_, blockFound := self.backend.GetStrong(strong)
			if blockFound {
				matchFound = true
				sgn.AddData(oldBlock[lastMatch:blockOffset])
				sgn.AddHash(weak, strong)
			}
		}
	}

}

func (self *Store) Get(id string, w io.Writer) (nb_bytes int){
	sgn, present := self.backend.GetSignature(id)
	if !present {
		return 0
	}
	sgn.Extract(self.backend, w)
	return 0 // USEFULL ?
}

func (self *Store) Put(id string, fd io.Reader) {
	sgn, err := self.GetSignature(fd)
	check(err)
	self.backend.SetSignature(id, sgn)
}

func (self *Signature) AddData(data []byte) {
	println("ADD DATA", len(data))
	segment := Segment{
		mode: DATA_SGM,
		data: data,
	}
	self.Segments = append(self.Segments, segment)
}

func (self *Signature) AddHash(weak WeakHash, strong *StrongHash) {
	println("ADD HASH")
	segment := Segment{
		mode: HASH_SGM,
		weakHash: weak,
		strongHash: strong,
	}
	self.Segments = append(self.Segments, segment)
}

func (self *Signature) Extract(backend Backend, w io.Writer) {
	for _, segment := range self.Segments {
		if segment.mode == DATA_SGM {
			w.Write(segment.data)
		} else if segment.mode == HASH_SGM {
			data, present := backend.GetStrong(segment.strongHash)
			if !present {
				panic("Hash not found in backend")
			}
			w.Write(data)
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
