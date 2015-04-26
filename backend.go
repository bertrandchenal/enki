package enki

import (
	"encoding/binary"
	"github.com/willf/bloom"
)

type Backend interface {
	AddBlock(WeakHash, *StrongHash, Block)
	SearchWeak(WeakHash) bool
	GetStrong(*StrongHash) (Block, bool)
	GetSignature(string) (*Signature, bool)
	SetSignature(string, *Signature)
}

type DummyBackend struct {
	BlockMap map[StrongHash]Block
	WeakMap map[WeakHash]bool
	SignatureMap map[string]*Signature
	bloomFilter *bloom.BloomFilter
}

func NewDummyBackend() Backend {
	backend := &DummyBackend{}
	// 14MB is the optimal size for 1MB of entries with false positive
	// rate of 0.001 (and 10 is the optimal number of functions)
	bloomCap := uint(14*2<<22)
	nbFunc := uint(10)
	backend.bloomFilter = bloom.New(bloomCap, nbFunc)
	backend.BlockMap = make(map[StrongHash]Block)
	backend.WeakMap = make(map[WeakHash]bool)
	backend.SignatureMap = make(map[string]*Signature)
	return backend
}

func (self *DummyBackend) AddBlock(weak WeakHash, strong *StrongHash, data Block) {
	_, present := self.BlockMap[*strong]
	if !present {
		weakb := make([]byte, 4)
		binary.LittleEndian.PutUint32(weakb, uint32(weak))
		self.bloomFilter.Add(weakb)
		self.WeakMap[weak] = true
		self.BlockMap[*strong] = data
		println("NEW STRONG")
	} else {
		println("DUP!")
	}
}

func (self *DummyBackend) GetStrong(strong *StrongHash) (Block, bool) {
	block, present := self.BlockMap[*strong]
	return block, present
}

func (self *DummyBackend) SearchWeak(weak WeakHash) bool {
	weakb := make([]byte, 4)
	binary.LittleEndian.PutUint32(weakb, uint32(weak))
	if self.bloomFilter.Test(weakb) {
		return self.WeakMap[weak]
	}
	return false
}

func (self *DummyBackend) GetSignature(id string) (*Signature, bool) {
	sgn, present := self.SignatureMap[id]
	return sgn, present
}

func (self *DummyBackend) SetSignature(id string, sgn *Signature) {
	self.SignatureMap[id] = sgn
}
