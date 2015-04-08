package enki

type Store interface {
	AddBlock(WeakHash, StrongHash, Block)
	SearchWeak(WeakHash) bool
	SearchStrong(StrongHash) bool
}

type DummyStore struct {
	BlockMap map[StrongHash]Block
	WeakMap map[WeakHash]bool
}

func (self DummyStore) AddBlock(weak WeakHash, strong StrongHash, data Block) {
	_, present := self.BlockMap[strong]
	if !present {
		println("STORE:ADDBLOCK")
		self.WeakMap[weak] = true
		self.BlockMap[strong] = data
	}
}

func (self DummyStore) SearchStrong(strong StrongHash) bool {
	//println("searchstrong")
	_, present := self.BlockMap[strong]
	return present
}

func (self DummyStore) SearchWeak(weak WeakHash) bool {
	//println("searchweark")
	return self.WeakMap[weak]
}
