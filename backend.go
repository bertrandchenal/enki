package enki

type Backend interface {
	AddBlock(WeakHash, *StrongHash, Block)
	SearchWeak(WeakHash) bool
	GetStrong(*StrongHash) Block
	GetSignature([]byte) *Signature
	SetSignature([]byte, *Signature)
	GetState(int64) *DirState
	SetState(*DirState)
	Close()
}
