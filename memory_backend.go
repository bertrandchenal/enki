package enki

type MemoryBackend struct {
	BlockMap     map[StrongHash]Block
	WeakMap      map[WeakHash]bool
	SignatureMap map[string]*Signature
	StateMap     map[int64]*DirState
	bloomFilter  *Bloom
}

func NewMemoryBackend() Backend {
	backend := &MemoryBackend{}
	backend.bloomFilter = NewBloom()
	backend.BlockMap = make(map[StrongHash]Block)
	backend.WeakMap = make(map[WeakHash]bool)
	backend.SignatureMap = make(map[string]*Signature)
	backend.StateMap = make(map[int64]*DirState)
	return backend
}

func (self *MemoryBackend) AddBlock(weak WeakHash, strong *StrongHash, data Block) {
	_, present := self.BlockMap[*strong]
	if !present {
		self.bloomFilter.Add(weak)
		self.WeakMap[weak] = true
		self.BlockMap[*strong] = data
	}
}

func (self *MemoryBackend) ReadStrong(strong *StrongHash) Block {
	block, present := self.BlockMap[*strong]
	if !present {
		return nil
	}
	return block
}

func (self *MemoryBackend) SearchWeak(weak WeakHash) bool {
	if self.bloomFilter.Test(weak) {
		return self.WeakMap[weak]
	}
	return false
}

func (self *MemoryBackend) ReadSignature(checksum []byte) *Signature {
	sgn, present := self.SignatureMap[string(checksum)]
	if !present {
		return nil
	}
	return sgn
}

func (self *MemoryBackend) WriteSignature(checksum []byte, sgn *Signature) {
	self.SignatureMap[string(checksum)] = sgn
}

func (self *MemoryBackend) ReadState(id int64) *DirState {
	st, present := self.StateMap[id]
	if !present {
		return nil
	}
	return st
}

func (self *MemoryBackend) WriteState(st *DirState) {
	self.StateMap[st.Timestamp] = st
}

func (self *MemoryBackend) Close() {
	// pass
}
