package enki

type Backend interface {
	AddBlock(WeakHash, *StrongHash, Block)
	SearchWeak(WeakHash) bool
	ReadStrong(*StrongHash) Block
	ReadSignature([]byte) *Signature
	WriteSignature([]byte, *Signature)
	ReadState(int64) *DirState
	WriteState(*DirState)
	Close()
}
