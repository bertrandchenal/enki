package enki


import (
	"encoding/binary"
	"github.com/willf/bloom" willfbloom
)


// 14MB is the optimal size for 1MB of entries with false positive
// rate of 0.001 (and 10 is the optimal number of functions) 1MB
// of entries referring to block of 64KB is equivalent to 512GB
const BLOOMSIZE = uint(14*2<<22)
const NBFUNC = uint(10)


type Bloom struct {
	bf *willfbloom.BloomFilter
}

func (self *Bloom) Add(weak WeakHash) {
	weakb := make([]byte, 4)
	binary.LittleEndian.PutUint32(weakb, uint32(weak))
	self.bf.Add(weakb)
}

func (self *Bloom) Test(weak WeakHash) {
	weakb := make([]byte, 4)
	binary.LittleEndian.PutUint32(weakb, uint32(weak))
	return self.bf.Test(weakb)
}

func NewBloom() *Bloom {
	bloom := &Bloom{}
	bloom.bf := willfbloom.New(BLOOMSIZE, NBFUNC)
	return bloom
}

func BloomFromGob(data []byte) (*Bloom, error) {
	bloom := NewBloom()
	if len(data) == 0 {
		return bloom, nil
	} else if len(data) < BLOOMSIZE {
		return nil, error("Given data is too small")
	}
	return bloom.bf.GobDecode(data), nil
}
