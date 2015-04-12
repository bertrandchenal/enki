package enki

import 	"encoding/hex"


type Store interface {
	AddBlock(WeakHash, *StrongHash, Block)
	SearchWeak(WeakHash) bool
	SearchStrong(*StrongHash) bool
}

type DummyStore struct {
	BlockMap map[StrongHash]Block
	WeakMap map[WeakHash]bool
}

func (self DummyStore) AddBlock(weak WeakHash, strong *StrongHash, data Block) {
	_, present := self.BlockMap[*strong]
	if !present {
		println("STORE:ADDBLOCK")
		self.WeakMap[weak] = true
		self.BlockMap[*strong] = data
	}
}

func (self DummyStore) SearchStrong(strong *StrongHash) bool {
	//println("searchstrong")
	_, present := self.BlockMap[*strong]
	if present {
		println("STORE:BLOCK MATCH")
	}
	println(" -- ",hex.Dump(*strong))
	return present
}

func (self DummyStore) SearchWeak(weak WeakHash) bool {
	//println("searchweark")
	res := self.WeakMap[weak]
	if res {
		println("WEAK FOUND", weak)
	}
	return res
}
