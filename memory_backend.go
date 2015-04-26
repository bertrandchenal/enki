package enki


type MemoryBackend struct {
	BlockMap map[StrongHash]Block
	WeakMap map[WeakHash]bool
	SignatureMap map[string]*Signature
	bloomFilter *Bloom
}

func NewMemoryBackend() Backend {
	backend := &MemoryBackend{}
	backend.bloomFilter = NewBloom()
	backend.BlockMap = make(map[StrongHash]Block)
	backend.WeakMap = make(map[WeakHash]bool)
	backend.SignatureMap = make(map[string]*Signature)
	return backend
}

func (self *MemoryBackend) AddBlock(weak WeakHash, strong *StrongHash, data Block) {
	_, present := self.BlockMap[*strong]
	if !present {
		self.bloomFilter.Add(weak)
		self.WeakMap[weak] = true
		self.BlockMap[*strong] = data
		println("NEW STRONG")
	} else {
		println("DUP!")
	}
}

func (self *MemoryBackend) GetStrong(strong *StrongHash) (Block, bool) {
	block, present := self.BlockMap[*strong]
	return block, present
}

func (self *MemoryBackend) SearchWeak(weak WeakHash) bool {
	if self.bloomFilter.Test(weak) {
		return self.WeakMap[weak]
	}
	return false
}

func (self *MemoryBackend) GetSignature(id string) (*Signature, bool) {
	sgn, present := self.SignatureMap[id]
	return sgn, present
}

func (self *MemoryBackend) SetSignature(id string, sgn *Signature) {
	self.SignatureMap[id] = sgn
}
